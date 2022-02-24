#!/usr/bin/env python
__doc__ = """TODO"""

import argparse
from common import JsonDict
from gcs_logging import log
from gcs_storage import get_change, store_stash
import io
import jsonlines


def store_stash_items(stash: JsonDict) -> int:
    """Return the number of stored items from the given stash."""

    stash_id: str = stash["id"]

    count = 0
    fp = io.BytesIO()
    with jsonlines.Writer(fp) as writer:
        items = stash.pop("items", [])
        for count, item in enumerate(items, start=1):
            stash_columns = {f"stash.{k}": v for k, v in stash.items()}
            item_columns = {f"item.{k}": v for k, v in item.items()}
            all_columns = {**stash_columns, **item_columns} # use | in Python >=3.9
            writer.write(all_columns)
    store_stash(stash_id, fp)

    #log(f"store_stash_items({stash_id}) -> {count}")
    return count


def store_stashes(change_id: str) -> int:
    """Return the number of stored items from the given stashes."""

    data = get_change(change_id)

    count = 0
    for stash in data.get("stashes", []):
        #print(f"stash={stash}")
        count += store_stash_items(stash)

    log(f"store_stashes({change_id}) -> {count}")
    return count


if __name__ == "__main__":
    """main()"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("change_id", help="What change id should we store?")
    args = parser.parse_args()
    store_stashes(args.change_id)
