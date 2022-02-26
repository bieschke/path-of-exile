#!/usr/bin/env python
__doc__ = """Write public stash changes to Google Cloud Storage."""

import argparse
from gcs_logging import log
import requests
import time
from typing import Optional

PUBLIC_STASH_TABS = "https://api.pathofexile.com/public-stash-tabs"
VERSION = "2"
CONTACT = "everyman@gmail.com"
HEADERS = {
    "User-Agent": f"OAuth bieschke/path-of-exile/{VERSION} (contact: {CONTACT}) StrictMode",
}


def public_stashes(change_id: Optional[str], retry_after: float) -> str:
    """Process some public stashes and return the next change."""

    if retry_after:
        log(f"sleeping for {retry_after:.0f} seconds")
        time.sleep(retry_after)

    params = {}
    if change_id:
        params["id"] = change_id
    response = requests.get(PUBLIC_STASH_TABS, params=params, headers=HEADERS)
    retry_after = float(response.headers.get("Retry-After", 0.0))

    if response.status_code in (200, 202):
        next_change_id, more = gcs_storage.store_change(change_id, response.text)
        if not more:
            log(f"stashes @ change {change_id}")
            return next_change_id # success! we're caught up
        return public_stashes(next_change_id, retry_after)

    if response.status_code == 429:
        return public_stashes(change_id, retry_after)

    raise RuntimeError(f"status_code={response.status_code} url={PUBLIC_STASH_TABS} params={params} error={response.text}")


if __name__ == "__main__":
    """main()"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("start-change-id", help="What change id should we start with?")
    args = parser.parse_args()
    public_stashes(args.start_change_id, 0.0)
