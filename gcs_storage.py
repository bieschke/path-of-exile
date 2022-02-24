from common import JsonDict
from gcs_logging import log
from google.cloud import storage
import io
import json
from typing import Tuple

BUCKET_STASHES_BY_CHANGE = "path-of-exile-public-stashes" # FIXME: .../change
BUCKET_STASHES = "path-of-exile-public-stashes" # FIXME


def store_change(change_id: str, data: str) -> Tuple[str, bool]:
    """Store a chunk of public stashes.

    Return the next change id and a boolean indicating whether or not
    there are more stashes currently available.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_STASHES_BY_CHANGE)
    blob = bucket.blob(change_id) # FIXME: f"{change_id}.json"
    blob.upload_from_string(data)
    log(blob.public_url)
    data_json: JsonDict = json.loads(data)
    return data_json["next_change_id"], bool(data_json["stashes"])


def get_change(change_id: str) -> JsonDict:
    """Return the JSON structure for the given change id."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_STASHES_BY_CHANGE)
    blob = bucket.blob(change_id)
    data = blob.download_as_text()
    return json.loads(data)


def store_stash(stash_id: str, fp: io.BytesIO) -> str:
    """Return the public URL of the stored stash."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_STASHES)
    blob = bucket.blob(f"{stash_id}.jsonl")
    blob.upload_from_file(fp, content_type="application/jsonlines")
    log(blob.public_url)
    return blob.public_url
