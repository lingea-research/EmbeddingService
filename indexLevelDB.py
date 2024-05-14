import logging
import plyvel   # "New BSD License" https://github.com/wbolster/plyvel/blob/main/LICENSE.rst

from os import path
from sys import stderr
from time import sleep

from indexDatabase import IndexDatabase

DB_CLOSE_TIMEOUT = 3


class IndexLevelDB(IndexDatabase):
    "per docs: multiple instances can be used concurrently in threads, but not across processes"
    INDEX_DB_DIRNAME = "indexDatabase"
    COMMIT_AFTER_CNT = 10   # arbitrary value, tune for speed & min data loss @ shutdown

    def __init__(self, data_dirpath: str = "", connection: plyvel._plyvel.DB = None):
        if connection is None:
            if not len(data_dirpath):
                raise ValueError("must provide 1 of data_dirpath or connection, neither given")
            self.db_path = path.join(data_dirpath, self.INDEX_DB_DIRNAME)
            self.connection = plyvel.DB(self.db_path, create_if_missing=True)
        else:
            self.db_path = None
            self.connection = connection
        print(f"***************** {self.connection}", file=stderr)
        self.write_batch = None
        self.cnt_put = 0

    # -------------------------------------------------------------------------
    def _int_to_bytes(self, c: int) -> bytes:
        return c.to_bytes(length=(max(c.bit_length(), 1) + 7) // 8)

    def create_table_if_not_exists(self) -> None:
        # LevelDB is already a key-value store
        return None

    def add_row(self, document_hash: str, offset: int) -> None:
        logging.info(f'add_row: recvd doc_hash "{document_hash}" | offset {offset}')
        if self.write_batch is None:
            self.write_batch = self.connection.write_batch()
        self.write_batch.put(document_hash.encode(), self._int_to_bytes(offset))
        self.cnt_put += 1
        if self.cnt_put >= self.COMMIT_AFTER_CNT:
            self.write_batch.write()
            self.cnt_put = 0
            self.write_batch = None

    def read_offset(self, document_hash: str) -> int | None:
        offset = self.connection.get(document_hash.encode())
        return None if offset is None else int.from_bytes(offset)

    # -------------------------------------------------------------------------
    def __del__(self) -> None:
        if self.connection is None or self.db_path is None: # this instance didn't open connection
            return
        for i in range(DB_CLOSE_TIMEOUT):
            sleep(1)
            self.connection.close()
            if self.connection.closed:
                logging.error(f"{__file__}: closed DB connection")
                return
        logging.error(f"{__file__}: failed to close DB after {DB_CLOSE_TIMEOUT} secs")

