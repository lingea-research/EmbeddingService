import logging
import requests

from argparse import Namespace
from filelock import Timeout, FileLock
from multiprocessing import Process
from multiprocessing.managers import SharedMemoryManager
from multiprocessing.shared_memory import ShareableList
from os import getpid, path, remove
from pickle import dumps
from psutil import pid_exists
from signal import signal, SIGINT, SIGTERM, SIG_IGN
from sys import exit, stderr
from tempfile import gettempdir
from threading import Thread
from time import sleep

from embeddingService import ACQUIRE_LOCK_TIMEOUT, EmbeddingService
from indexDatabase import IndexDatabase
from indexSQLite import IndexSQLite
from indexLevelDB import IndexLevelDB

DCP_BUSY_WAIT_SLEEP_SECS = 0.005 # arbitrary & tunable, less: more responsive, more: busier CPU
WAIT_UVICORN_UP_TIMEOUT_SECS = 20   # time needed for workers to report their PIDs
WAIT_SHMS_UP = 30   # also arbitrary, but less important, just don't set too low


class SHMPayload:   # a ShareableList item
    "[-------------------digest-------------|---offset---]"
    DUMMY_DIGEST = EmbeddingService.get_hash("a")
    DUMMY_OFFSET = hex(2**63 - 1)# 64-bit integer
    DUMMY_PAYLOAD = DUMMY_DIGEST + DUMMY_OFFSET # "init" ShareableList item to max len it can store

    def __init__(self, digest: str = None, offset: int = None, string: str = None):
        self.digest = digest
        self.offset = offset
        self.string = string
        self.me = self.__class__.__name__
    def pack(self) -> str | None:
        if self.digest is None or self.offset is None:
            logging.error(f"{self.me}: cannot pack({self.digest},{self.offset})")
            return None
        self.string = self.digest + hex(self.offset) # this assumes digest will always be same len
        return self.string
    def unpack(self) -> tuple[str,int]:
        error_msg = f"{self.me}: cannot unpack({self.string})"
        if (self.string is None or
                len(self.string) < len(self.DUMMY_DIGEST) + 3):    # "+3" because len("0x[0-9]")
            logging.error(error_msg)
            return None
        hash_len = len(self.DUMMY_DIGEST)
        try:
            return (self.string[:hash_len], int(self.string[hash_len:], 16))
        except ValueError as e:
            logging.error(error_msg + ": " + str(e))
            return None


class DatabaseCommitProcess(Process):
    SUPPORTED_DB_TYPES = ["leveldb", "sqlite"]
    SHM_NAME_PREFIX = "DatabaseCommitProcessSHM"
    WORKER_SHM_SIZE = 15 # length of ShareableList per worker (named: SHM_NAME_PREFIX + pid)
    WORKER_PIDS_FILE = path.join(gettempdir(), "DatabaseCommitProcess_pids")
    WORKER_PIDS_LOCK = WORKER_PIDS_FILE + ".lock"
    SENTINEL_OFFSET = int(SHMPayload.DUMMY_OFFSET, 16)
    SENTINEL_DIGEST = len(SHMPayload.DUMMY_DIGEST) * "\x15"
    elapsed = 0

    def __init__(self, args: Namespace):
        super().__init__()
        self.me = self.__class__.__name__
        self.cnt_workers = args.workers
        self.shm_lists = {}
        self.shm_mgr = SharedMemoryManager()
        self.shm_mgr.start()
        model_dirpath = EmbeddingService.get_model_dirpath(args.data_dir, args.model)
        self.db_type = args.db_type
        if self.db_type not in self.SUPPORTED_DB_TYPES:
            logging.error(f'cannot use "{self.db_type}", for now only support: {self.SUPPORTED_DB_TYPES}')
            raise ValueError
        if self.db_type == "sqlite":
            self.database_rw = IndexSQLite(model_dirpath, readonly = False)
        elif self.db_type == "leveldb":
            self.database_rw = IndexLevelDB(model_dirpath)

    # --------------------------------------------------------------------------
    @staticmethod
    def get_shm_name(pid: int) -> str:
        return DatabaseCommitProcess.SHM_NAME_PREFIX + str(pid) # TODO: is this unique enough?

    # --------------------------------------------------------------------------
    def _get_worker_pids(self) -> list[int]:
        with open(self.WORKER_PIDS_FILE, "w"):
            pass
        worker_pids = []
        # TODO: if lock not acq'd immediately, timeout is not actually WAIT_UVICORN_UP_TIMEOUT_SECS
        for i in range(WAIT_UVICORN_UP_TIMEOUT_SECS):
            lawk = FileLock(self.WORKER_PIDS_LOCK, timeout = ACQUIRE_LOCK_TIMEOUT)
            try:
                lawk.acquire()
            except Timeout:
                logging.error(f"{self.me}: acquire WORKER_PIDS_LOCK timed-out after "
                        f"{ACQUIRE_LOCK_TIMEOUT}s")
                raise Timeout
            else:
                with open(self.WORKER_PIDS_FILE, "r") as rfp:
                    worker_pids = [int(line.strip()) for line in rfp]
                lawk.release()
            if len(worker_pids) >= self.cnt_workers:
                logging.info(f"uvicorn workers unite! {str(worker_pids)}")
                remove(self.WORKER_PIDS_FILE)
                return worker_pids
            sleep(1)
        logging.error(f"{self.me}: _wait_uvicorn_up timed-out after "
                f"{WAIT_UVICORN_UP_TIMEOUT_SECS}s")
        raise TimeoutError

    # --------------------------------------------------------------------------
    def run(self):
        logging.info(f"starting database commit process {getpid()}")
        temp_shms = {}
        for pid in self._get_worker_pids():
            self.shm_lists[pid] = self.shm_mgr.ShareableList(
                    [SHMPayload.DUMMY_PAYLOAD] * self.WORKER_SHM_SIZE)
            temp_shms[pid] = ShareableList(
                    [dumps(self.shm_lists[pid])], name=self.get_shm_name(pid))

        logging.info(f"{self.me}: waiting for workers to rcv shm location "
                f"(timeout={WAIT_SHMS_UP}s)")
        sleep_interval = 0.01
        elapsed = 0
        while elapsed < WAIT_SHMS_UP:
            for p in list(temp_shms.keys()):
                if not len(self.shm_lists[p][0]):
                    temp_shms[p].shm.close()
                    temp_shms[p].shm.unlink()
                    del temp_shms[p]
            if len(temp_shms) == 0:
                break
            elapsed += sleep_interval
            sleep(sleep_interval)
        if len(temp_shms):
            for p in list(temp_shms.keys()):
                temp_shms[p].shm.close()
                temp_shms[p].shm.unlink()
            logging.error(f"{self.me}: timed-out wait_shms_up after {WAIT_SHMS_UP}s, au revoir...")
            self.clean_up()
            raise TimeoutError

        threads = []
        for pid, shm in self.shm_lists.items():
            if self.db_type == "leveldb":
                db_obj = IndexLevelDB(connection=self.database_rw.connection)
            else:
                db_obj = self.database_rw
            t = Thread(target=db_thread, args=[pid, shm, db_obj])
            t.start()
            threads.append(t)

        try:
            for t in threads:
                t.join()
        finally:
            self.clean_up()

    # --------------------------------------------------------------------------
    def clean_up(self):
        if len(self.shm_lists) == 0:    # nothing todo
            return
        # avoid exiting until resources cleaned-up
        signal(SIGINT, SIG_IGN)
        signal(SIGTERM, SIG_IGN)

        for shm in self.shm_lists.values():
            shm.shm.close()
        # manager will handle cleaning-up shmem
        self.shm_mgr.shutdown()

# --------------------------------------------------------------------------
def db_thread(pid: int, shm: ShareableList, db_obj: IndexDatabase) -> None:
    # main loop: wait.. rcv.. process..
    while True:
        msg_ind = None
        for i in range(DatabaseCommitProcess.WORKER_SHM_SIZE):
            if len(shm[i]):
                msg_ind = i
                break
        if msg_ind is None:
            sleep(DCP_BUSY_WAIT_SLEEP_SECS)
            continue

        digest, offset = SHMPayload(string = shm[msg_ind]).unpack()
        # if shm msg is type "read"
        if isinstance(db_obj, IndexLevelDB) and offset == DatabaseCommitProcess.SENTINEL_OFFSET:
            val = db_obj.read_offset(digest)
            if val is None:
                reply = SHMPayload(
                        DatabaseCommitProcess.SENTINEL_DIGEST,
                        DatabaseCommitProcess.SENTINEL_OFFSET).pack()
            else:
                reply = SHMPayload(DatabaseCommitProcess.SENTINEL_DIGEST, val).pack()
        else:
            val = db_obj.add_row(digest, offset)
            reply = ""
        #print(f"========= add_row returned {val}:\n\t{msg}", file=stderr)

        shm[msg_ind] = reply

