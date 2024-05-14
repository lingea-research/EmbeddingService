"""
Class for computing and storing word embeddings.
 Use the get_embeddings method to compute what you need.
"""
import argparse
import logging
import numpy as np
import os
from struct import pack, unpack

from filelock import Timeout, FileLock
from hashlib import sha256
from tempfile import gettempdir

from model import Model

_SCRIPT_NAME_ = os.path.basename(__file__)
ACQUIRE_LOCK_TIMEOUT = 59  # secs
MODELS_CFG_FILENAME = "models.txt"
EMBEDDINGS_FILENAME = "embeddings.bin"


class EmbeddingService:
    def __init__(self, args: argparse.Namespace):
        # models & locks are keyed on full model name
        self.models = dict()
        self.locks = dict()
        self.datadir = args.data_dir
        self.db_type = args.db_type
        self.models_cfg = None
        self.load_models()

    @staticmethod
    def get_lock_dirpath() -> str:
        return os.path.join(gettempdir(), _SCRIPT_NAME_)

    @staticmethod
    def get_model_dirpath(data_dirpath: str, model_name: str) -> str:
        return os.path.join(data_dirpath, EmbeddingService.normalize_model_dirname(model_name))

    @staticmethod
    def normalize_model_dirname(model_name: str) -> str:
        "replaces filesystem path sep with underscore in-case model 'name' has a path of its own"
        return model_name.replace(os.path.sep, "_")

    def get_lock_filepath(self, model_name: str) -> str:
        lock_name = EmbeddingService.normalize_model_dirname(model_name)
        return os.path.join(EmbeddingService.get_lock_dirpath(), f"{lock_name}.lock")

    def get_binpath(self, model_name: str = "") -> str:
        return os.path.join(
                EmbeddingService.get_model_dirpath(self.datadir, model_name),
                EMBEDDINGS_FILENAME)
    # -------------------------------------------------------------------------
    @staticmethod
    def get_hash(document: str) -> str:
        # print(document)
        return sha256(bytes(document, encoding="utf-8")).hexdigest()

    # -------------------------------------------------------------------------
    @staticmethod
    def get_models_cfg(data_dirpath: str) -> dict:
        models_cfg = {}
        with open(MODELS_CFG_FILENAME, "r", encoding="utf-8") as f:

            for ln, line in enumerate(f.read().split(os.linesep), 1):
                parts = line.split()
                # skip empty/malformed lines and "comments"
                if len(parts) < 3 or parts[0][1] == "#":
                    if len(line) != 0:
                        logging.warning(f'skipping models config line #{ln}: "{line}"')
                    continue
                name, embedding_dimension = parts[0], int(parts[1])
                model_data_dir = EmbeddingService.get_model_dirpath(data_dirpath, name)
                models_cfg[name] = {
                        "embedding_dimension": embedding_dimension, "data_dirpath": model_data_dir,
                        "autoload": parts[2] != "0",
                        }
        return models_cfg

    # -------------------------------------------------------------------------
    @staticmethod
    def setup_model_dir(cfg: dict) -> None:
        model_data_dir = cfg["data_dirpath"]
        if not os.path.exists(model_data_dir):
            try:
                os.makedirs(model_data_dir)
            except FileExistsError:
                pass    # ok so long as somebody makedirs.. 8-}

    # -------------------------------------------------------------------------
    @staticmethod
    def setup_models_dirs(cfg: dict) -> None:
        for model_cfg in cfg.values():
            EmbeddingService.setup_model_dir(model_cfg)

    # -------------------------------------------------------------------------
    def load_models(self) -> None:
        self.models_cfg = EmbeddingService.get_models_cfg(self.datadir)
        for name, cfg in self.models_cfg.items():
            if cfg["autoload"]: #   skip models which should not be loaded on start
                self.load_model(name, cfg)

    # -------------------------------------------------------------------------
    def load_model(self, name: str, cfg: dict) -> None:
        EmbeddingService.setup_model_dir(cfg)
        cache_file_path = self.get_binpath(name)
        if not os.path.exists(cache_file_path):
            with open(cache_file_path, "wb") as f:
                pass
        self.models[name] = Model(name, cfg["embedding_dimension"],
                                  cfg["data_dirpath"], self.db_type)

    # -------------------------------------------------------------------------
    def get_embeddings(self, document: str, model_name: str, read_cache: bool = True
    ) -> tuple[np.ndarray, list | None]:
        document_hash = EmbeddingService.get_hash(document)
        model = self.models[model_name]

        to_write = None
        if read_cache:
            offset = model.read_offset(document_hash)
            if offset is not None:
                # return np.array([0])
                return self.read_embeddings(offset, model, self.get_binpath(model_name)), to_write

        embeddings = model.compute_embeddings(document)
        # the to_write list is used by caller to write_embeddings in the BG after the response
        # has been sent
        to_write = [embeddings, document_hash, model]
        return embeddings, to_write

    # -------------------------------------------------------------------------
    def _write_embeddings(self, packed_data: bytes, document_hash: str, model: Model) -> int:
        offset = -1
        with open(self.get_binpath(model.name), "rb+") as f:
            f.seek(0, 2)        # move file pointer to end of file
            offset = f.tell()
            f.write(packed_data)
        return offset

    # -------------------------------------------------------------------------
    def write_embeddings(self, embedding: list, document_hash: str, model: Model) -> None:
        """
        writes the computed word embeddings into a file.
        Also stores the offset information into the model index.
        """
        format_string = f"{len(embedding)}f"
        packed_data = pack(format_string, *embedding)
        lock_dir = EmbeddingService.get_lock_dirpath()

        if not os.path.exists(lock_dir):
            try:
                os.mkdir(lock_dir)
            except FileExistsError:
                pass    # lost the race to another worker?
        if model.name not in self.locks:
            self.locks[model.name] = FileLock(self.get_lock_filepath(model.name),
                                              timeout=ACQUIRE_LOCK_TIMEOUT)
        try:
            self.locks[model.name].acquire()
        except Timeout:
            logging.error(f'timed-out acquiring lockfile to write to "{bin_filepath}", giving up...')
            return

        offset = self._write_embeddings(packed_data, document_hash, model)
        self.locks[model.name].release()

        # TODO: add consistentcy chk @ start-up in case app exits after cache wr, but b4 DB insert
        # note: only consequence for above TODO would probably be a "lost" cached embedding in file
        # reverse order (DB insert b4 cache wr) seems to have worse consequence (data corruption)
        model.write_offset(document_hash, offset)

    # -------------------------------------------------------------------------
    def read_embeddings(self, offset: int, model: Model, path: str) -> np.ndarray:
        "reads the embeddings from a cache file"
        with open(path, "rb") as f:
            f.seek(offset)
            embedding = [0] * model.embedding_dimension
            for i in range(model.embedding_dimension):
                embedding[i] = unpack("f", f.read(4))[0]
            return np.array(embedding, dtype=np.float32)

