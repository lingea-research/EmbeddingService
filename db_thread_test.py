import duckdb
import plyvel
import sqlite3
from hashlib import sha256
from math import ceil
from multiprocessing import Process
from os import path
from sys import argv, exit
from time import sleep, time
from threading import Lock, Thread
from random import random, seed

COMMIT_FREQ = 10
DB_DIRPATH = "/dev/shm"

seed(time())

def usage():
    print(f"usage: {path.basename(__file__)} <duckdb|leveldb|sqlite>")
    exit(0)

def get_bytes(n):
    return n.to_bytes(length=(max(n.bit_length(), 1) + 7) // 8)

def rand_row():
    n = int(ceil(random() * 100000))
    return sha256(str(n).encode()).hexdigest(), n

def level_t(i, c, lock):
    wb = None
    for j in range(101):
        if wb is None:
            wb = c.write_batch()
        s, n = rand_row()
        wb.put(s.encode(), get_bytes(n))
        if not (j % COMMIT_FREQ):
            wb.write()
            wb = None
    lock.acquire()
    for key, value in c:
        print(f"{i}: ", bytes.decode(key), " | ", int.from_bytes(value))
    lock.release()

def sqlite_t(i, c):
    cursor = c.cursor()
    w_query = 'INSERT OR IGNORE INTO OffsetIndex (documentHash, offset) VALUES (?, ?)'
    for j in range(101):
        cursor.execute(w_query, rand_row())
        if not (j % COMMIT_FREQ):
            c.commit()
    r_query = "SELECT * FROM OffsetIndex WHERE 1"
    cursor.execute(r_query)
    for row in cursor.fetchall():
        print(f"{i}: ", row)

def duckdb_t(i, c, lock):
    wb = None
    cursor = c.cursor()
    w_query = 'INSERT OR IGNORE INTO OffsetIndex (documentHash, _offset) VALUES (?, ?)'
    for j in range(101):
        if wb is None:
            wb = cursor.begin()
        s, n = rand_row()
        cursor.execute(w_query, rand_row())
        if not (j % COMMIT_FREQ):
            cursor.commit()
    r_query = "SELECT * FROM OffsetIndex WHERE 1"
    result = cursor.execute(r_query)
    lock.acquire()
    for row in result.fetchall():
        print(f"{i}: ", row)
    lock.release()

if len(argv) != 2:
    usage()
if argv[1] == "leveldb":
    c = plyvel.DB(f"{DB_DIRPATH}/indexDatabase", create_if_missing=True)
    args = [c, Lock()]
    target = level_t
elif argv[1] == "sqlite":
    c = sqlite3.connect(f"{DB_DIRPATH}/indexDatabase.db", isolation_level="DEFERRED")
    query = 'CREATE TABLE IF NOT EXISTS OffsetIndex (documentHash TEXT PRIMARY KEY, offset INTEGER)'
    c.cursor().execute(query)
    c.commit()
    args = [c]
    target = sqlite_t
elif argv[1] == "duckdb":
    c = duckdb.connect(f"{DB_DIRPATH}/indexDatabase.duckdb")
    query = 'CREATE TABLE IF NOT EXISTS OffsetIndex (documentHash VARCHAR PRIMARY KEY, _offset INT)'
    c.execute(query)
    args = [c, Lock()]
    target = duckdb_t
else:
    usage()

t = []
for i in range(4):
    clazz = Process if argv[1] == "sqlite" else Thread
    t.append(clazz(target=target, args=[i, *args]))
    t[i].start()
for t in t:
    t.join()

c.close()

