
import sys

with open("uniq_index.txt", "w", encoding="utf-8") as f:
    prev = None
    for line in sys.stdin:
        split_line = line.split(" ")
        if split_line[0] != prev:
            prev = split_line[0]
            f.write(line)

