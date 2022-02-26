#!/usr/bin/env python
__doc__ = """Write public stash items to Google Cloud Storage."""

import argparse
from common import JsonDict
from gcs_logging import log
from gcs_storage import get_change, store_stash
import io
import jsonlines


def format_for_bigquery(item: dict) -> dict:
    """Return the given item formatted for BigQuery.

    This function removes columns that we haven't translated
    for BigQuery to understand. BigQuery blows up on nested
    arrays for example when using auto schema generation.
    """
    skip = (
        "item_additionalProperties",
        "item_hybrid",
        "item_nextLevelRequirements",
        "item_notableProperties",
        "item_properties",
        "item_requirements",
        "item_socketedItems",
        "item_ultimatumMods",
    )
    return { k:v for k,v in item.items() if k not in (skip) }


def store_stash_items(stash: JsonDict) -> int:
    """Return the number of stored items from the given stash."""

    stash_id: str = stash["id"]

    count = 0
    fp = io.BytesIO()
    with jsonlines.Writer(fp) as writer:
        items = stash.pop("items", [])
        for count, item in enumerate(items, start=1):
            stash_columns = {f"stash_{k}": v for k, v in stash.items()}
            item_columns = {f"item_{k}": v for k, v in item.items()}
            all_columns = {**stash_columns, **item_columns} # use | in Python >=3.9
            writer.write(format_for_bigquery(all_columns))
    if count:
        store_stash(stash_id, fp)

    #log(f"store_stash_items({stash_id}) -> {count}")
    return count


def store_stashes(change_id: str) -> int:
    """Return the number of stored items from the given change."""

    data = get_change(change_id)

    count = 0
    for stash in data.get("stashes", []):
        #print(f"stash={stash}")
        count += store_stash_items(stash)

    return count


def public_stashes_finalize(event, context):
    """Triggered by a finalize to a Cloud Storage bucket.
    Finalizes occur every time a file is created/overwritten.

    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")

    change_id = event["name"].removesuffix(".json")
    count = store_stashes(change_id)
    log(f"store_stashes({change_id}) -> {count}")


if __name__ == "__main__":
    """main()"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("change_id", help="What change id should we store?")
    args = parser.parse_args()
    store_stashes(args.change_id)
