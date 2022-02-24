#!/usr/bin/env python
__doc__ = """Store public stashes in Google Cloud Storage."""

import argparse
from gcs_logging import log
from gcs_storage import store_change
import requests
import time

PUBLIC_STASH_TABS = "https://api.pathofexile.com/public-stash-tabs"
VERSION = "1"
CONTACT = "everyman@gmail.com"
HEADERS = {
    "User-Agent": f"OAuth bieschke/path-of-exile/{VERSION} (contact: {CONTACT}) StrictMode",
}
UP2DATE_DELAY = 60 * 5 # 5 minutes


def public_stashes(change_id: str, retry_after: float):
    """Process a chunk of public stashes."""

    if retry_after:
        log(f"sleeping for {retry_after:.0f} seconds")
        time.sleep(retry_after)

    response = requests.get(PUBLIC_STASH_TABS, params={"id": change_id}, headers=HEADERS)

    if response.status_code in (200, 202):
        next_change_id, more = store_change(change_id, response.text)
        retry_after = more and 0.0 or UP2DATE_DELAY
        return public_stashes(next_change_id, retry_after)

    if response.status_code == 429:
        retry_after = float(response.headers.get("Retry-After", 0.0))
        return public_stashes(change_id, retry_after)

    raise RuntimeError(f"status_code={response.status_code} url={PUBLIC_STASH_TABS} params={params} error={response.text}")


if __name__ == "__main__":
    """main()"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-change-id", default="START", help="What change id should we start with?")
    args = parser.parse_args()
    public_stashes(args.start_change_id, 0.0)
