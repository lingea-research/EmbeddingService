
import sys

def get_path(index):
    if index == "B":
        return "embeddings/data/backup/embeddings.bin"
    return "embeddings/data/embeddings/test/" + index + "/embeddings.bin"

with open("embedding_index.idx", "w", encoding="utf-8") as f1:
    with open("embedding_data.bin", "wb") as f2:
        n = 0
        for line in sys.stdin:
            parts = line[:-1].split(" ")
            doc_hash = parts[0]
            offset = parts[1]
            index = parts[2]

            with open(get_path(index), "rb") as f:
                f.seek(int(offset))
                f2.write(f.read(2048))
                f1.write(doc_hash + " " + str(n * 2048) + "\n")
                n += 1

