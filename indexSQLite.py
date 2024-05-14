import logging
import sqlite3
from os import path

from indexDatabase import IndexDatabase


class IndexSQLite(IndexDatabase):
    INDEX_DB_FILE = "indexDatabase.db"
    COMMIT_AFTER_CNT = 10   # arbitrary value, tune for speed & min data loss @ shutdown

    def __init__(self, dirpath: str, readonly: bool = True):
        self.db_filepath = path.join(dirpath, self.INDEX_DB_FILE)
        self.trans_cnt = 0
        self.readonly = readonly

        uri = f"file:{self.db_filepath}" + ("?mode=ro" if readonly else "")
        try:
            self.connection = sqlite3.connect(uri,
                    check_same_thread=False, uri=True, isolation_level="DEFERRED")
        except sqlite3.OperationalError as e:
            logging.error(f'failed to connect to database: "{str(e)}"')
            self.connection = None
            raise ConnectionError

        self.cursor = self.connection.cursor()
        if not readonly:
            self.create_table_if_not_exists()

    # -------------------------------------------------------------------------
    def create_table_if_not_exists(self) -> bool:
        if self.readonly:
            return False
        query = 'CREATE TABLE IF NOT EXISTS OffsetIndex (documentHash TEXT PRIMARY KEY, offset INTEGER)'
        self.cursor.execute(query)
        self.connection.commit()

    # -------------------------------------------------------------------------
    def add_row(self, document_hash: str, offset: int) -> bool:
        # TODO: add try/except block to db calls that may fail
        if self.readonly:
            return False
        #if document_hash not in self.temp_index:
        #    self.temp_index[document_hash] = offset
        query = 'INSERT OR IGNORE INTO OffsetIndex (documentHash, offset) VALUES (?, ?)'
        values = (document_hash, offset)
        logging.debug(f"add_row: values = {str(values)}")
        self.cursor.execute(query, values)

        self.trans_cnt += 1
        if self.trans_cnt >= self.COMMIT_AFTER_CNT:
            self.connection.commit()    # per docs "BEGIN DEFERRED" after commit() is implied
            self.trans_cnt = 0
        return True

    # -------------------------------------------------------------------------
    def read_offset(self, document_hash: str) -> int | None:
        query = 'SELECT offset FROM OffsetIndex WHERE documentHash = ?'
        self.cursor.execute(query, (document_hash,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        return None
        #if document_hash in self.temp_index:
        #    return self.temp_index[document_hash]
        #return None

    # -------------------------------------------------------------------------
    def __del__(self) -> None:
        if self.connection is None:
            return
        if not self.readonly:
            self.connection.commit()
        self.cursor.close()
        self.connection.close()

    # -------------------------------------------------------------------------

    """
    def read_all(self) -> None:
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        cursor.execute('SELECT DocumentHash, Offset FROM OffsetIndex')
        rows = cursor.fetchall()

        for row in rows:
            document_hash, offset = row
            self.temp_index[document_hash] = offset
        conn.close()
    def write_temp_index(self) -> None:
        self.connection = sqlite3.connect(self.db_name)
        self.cursor = self.connection.cursor()
        self.create_table_if_not_exists()

        query = 'INSERT OR IGNORE INTO OffsetIndex (documentHash, offset) VALUES (?, ?)'
        for document_hash in self.temp_index:
            values = (document_hash, self.temp_index[document_hash])
            self.cursor.execute(query, values)
            self.connection.commit()
        # self.temp_index.clear()

        self.cursor.close()
        self.connection.close()
    """

