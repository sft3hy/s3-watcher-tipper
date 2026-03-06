import json
from analyze import load_and_fuse


def debug():
    data = load_and_fuse(
        "part-00001-26f8f250-5441-4cc2-8b0e-238d0e6e1a61.c000.snappy.csv"
    )
    print("TIMELINE LENGTH:", len(data["timeline"]))
    if data["timeline"]:
        print("TIMELINE SAMPLE:", data["timeline"][:2])

    print("GEO LENGTH:", len(data["geo"]))
    if data["geo"]:
        print("GEO SAMPLE:", data["geo"][:2])


if __name__ == "__main__":
    debug()
