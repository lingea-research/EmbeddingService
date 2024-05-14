"""
represents a neural model used for computing word embeddings
"""
import logging

from multiprocessing.shared_memory import ShareableList
from numpy import ndarray
from os import getpid, linesep
from pickle import loads
#from sentence_transformers import SentenceTransformer  # loaded in __init__ below
from signal import signal, SIGINT, SIGTERM
from sys import exit
from time import sleep

import databaseCommitProcess as dcp
from indexLevelDB import IndexLevelDB
from indexSQLite import IndexSQLite

INIT_SHM_TIMEOUT = 10   # secs
READ_SHM_TIMEOUT = 5
READ_SHM_POLL_INTERVAL1 = 0.001
READ_SHM_POLL_INTERVAL2 = 0.005


class Model:
    def __init__(self, name: str, embedding_dimension: int, data_dirpath: str,
                 db_type: str, load_transformers: bool = True):
        self.name = name
        self.embedding_dimension = embedding_dimension  # how many floats the embeddings has
        self.data_dirpath = data_dirpath
        self.db_type = db_type
        if db_type == "sqlite":
            self.database_ro = IndexSQLite(data_dirpath, readonly = True)
        else:
            self.database_ro = None
        self._init_db_shm()
        signal(SIGINT, self.clean_up)
        signal(SIGTERM, self.clean_up)

        if load_transformers:   # debug hack, speeds up runs that test non-model features when F
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer(name)  # TODO: any exceptions to catch here?
        else:
            self.model = None
        self.load_transformers = load_transformers

    # --------------------------------------------------------------------------
    def _init_db_shm(self) -> None:
        elapsed = 0
        sleep_interval = 0.1
        while elapsed < INIT_SHM_TIMEOUT:
            try:
                temp_shm = ShareableList(name = dcp.DatabaseCommitProcess.get_shm_name(getpid()))
            except FileNotFoundError:
                elapsed += sleep_interval
                sleep(sleep_interval)
            else:
                while elapsed < INIT_SHM_TIMEOUT:
                    if len(temp_shm):
                        self.db_shm = loads(temp_shm[0])
                        temp_shm.shm.close()
                        for i in range(dcp.DatabaseCommitProcess.WORKER_SHM_SIZE):
                            self.db_shm[i] = ""   # signal to DCP that SHM location was rcvd
                        return
                    sleep(sleep_interval)
        raise TimeoutError

    # --------------------------------------------------------------------------
    def compute_embeddings(self, document: str) -> ndarray:
        return self.model.encode(document) if self.load_transformers else ndarray([[0],[0],[0],[0]])

    # --------------------------------------------------------------------------
    def read_offset(self, document_hash: str) -> int | None:
        if self.database_ro is not None:
            return self.database_ro.read_offset(document_hash)
        if self.db_type == "leveldb":
            val = self.send_shm_msg(document_hash,
                                    dcp.DatabaseCommitProcess.SENTINEL_OFFSET, get_reply=True)
            if isinstance(val, bool) and not val:
                return None
            if isinstance(val, tuple):
                return int(val[1])
            return None

    def write_offset(self, document_hash: str, offset: int) -> bool:
        return self.send_shm_msg(document_hash, offset)

    # --------------------------------------------------------------------------
    def send_shm_msg(self, document_hash: str, offset: int, get_reply: bool = False
    ) -> bool | tuple[str, int]:
        msg = dcp.SHMPayload(document_hash, offset).pack()
        if msg is None:
            logging.error("send_shm_msg: can't create msg offset={offset}, hash:'{document_hash}'")
            return False
        try:
            available_ind = self.db_shm.index("")
        except ValueError as e:
            logging.warning(f"send_shm_msg: no room to write to db_shm: {str(e)}, dropping"
                            f"{linesep}\toffset={offset}, hash:'{document_hash}'")
            return False
        self.db_shm[available_ind] = msg

        if not get_reply:
            return True
        elapsed = READ_SHM_POLL_INTERVAL1
        sleep(READ_SHM_POLL_INTERVAL1)

        while elapsed < READ_SHM_TIMEOUT:
            inbox = self.db_shm[available_ind]
            digest = inbox[:len(dcp.DatabaseCommitProcess.SENTINEL_DIGEST)]
            if digest == dcp.DatabaseCommitProcess.SENTINEL_DIGEST:
                reply = dcp.SHMPayload(string=inbox).unpack()
            else:
                continue
            if reply is None:
                logging.error("send_shm_msg: can't unpack reply")
                return False
            else:
                return reply
            sleep(READ_SHM_POLL_INTERVAL2)
            elapsed += READ_SHM_POLL_INTERVAL

    # --------------------------------------------------------------------------
    def clean_up(self, signum=None, frame=None):
        if self.db_shm is not None:
            self.db_shm.shm.close()
            if self.db_shm.shm.buf is None:
                logging.info(f"worker {getpid()} closed shm successfully.")
                self.db_shm = None
    # --------------------------------------------------------------------------
    """
    def write_temp_index(self) -> None:
        self.get_index_database().write_temp_index()
    """

