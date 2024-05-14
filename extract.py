
import numpy as np
import sqlite3
import struct

def extract(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[0]
        create_statement = table[1]
        print("Table: {}".format(table_name))
        print(create_statement)
        print()

    cursor.execute("SELECT * FROM OffsetIndex")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def read_embeddings(offset: int, path: str):
    with open(path, "rb") as f:
        f.seek(offset)
        embedding = [0] * 512
        for i in range(512):
            embedding[i] = struct.unpack("f", f.read(4))[0]
        return np.array(embedding, dtype=np.float32)

def write_embeddings(embedding: list, document_hash: str, path: str, connection, cursor):
    format_string = f"{len(embedding)}f"
    packed_data = struct.pack(format_string, *embedding)

    with open(path, "rb+") as f:
        f.seek(0, 2)
        offset = f.tell()
        f.write(packed_data)

    print("Inserting", document_hash)
    query = 'INSERT INTO OffsetIndex (documentHash, offset) VALUES (?, ?)'
    values = (document_hash, offset)
    cursor.execute(query, values)
    connection.commit()

def main():

    with open("index.txt", "w", encoding="utf-8") as f:
        for i in range(8):
            print("Extracting data1")
            data = extract("embeddings/data/test/" + str(i) + "/indexDatabase.db")
            print("Read", len(data), "lines")

            for k, v in data:
                f.write(str(k) + " " + str(v) + " " + str(i) + "\n")

        print("Extracting data1")
        data = extract("embeddings/data/backup/indexDatabase.db")
        print("Read", len(data), "lines")

        for k, v in data:
            f.write(str(k) + " " + str(v) + " B\n")


    """connection = sqlite3.connect("embeddings/data/test/0/indexDatabase.db")
    cursor = connection.cursor()
    #query = 'CREATE TABLE IF NOT EXISTS OffsetIndex (documentHash TEXT PRIMARY KEY, offset INTEGER)'
    #cursor.execute(query)
    #connection.commit()

    i = 0
    for hash_, offset in data:
        embeddings = read_embeddings(offset, "embeddings/data/embeddings/test/1/embeddings.bin")
        write_embeddings(embeddings, hash_, "embeddings/data/embeddings/test/0/embeddings.bin", connection, cursor)

    cursor.close()
    connection.close()"""

main()






