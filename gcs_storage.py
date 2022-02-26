from common import JsonDict
from gcs_logging import log
from google.cloud import storage
import io
import json
from typing import Optional, Tuple

CHANGE_BUCKET = "path-of-exile-public-stashes-by-change"
STASH_BUCKET = "path-of-exile-public-stashes-by-stash"

storage_client = storage.Client()


def get_next_change_id() -> Optional[str]:
    """Return the next change id.

    None is returned if the previous value isn't there."""
    bucket = storage_client.bucket(CHANGE_BUCKET)
    blob = bucket.blob(NEXT_CHANGE_ID)
    return blob.download_as_text() or None


def set_next_change_id(change_id: str) -> str:
    """Set the next change id and return it's public URL."""
    bucket = storage_client.bucket(CHANGE_BUCKET)
    blob = bucket.blob(NEXT_CHANGE_ID)
    blob.upload_from_string(change_id)
    log(blob.public_url)
    return blob.public_url


def store_change(change_id: str, data: str) -> Tuple[str, bool]:
    """Store the JSON for the given change.

    Return the next change id and a boolean indicating whether or not
    there are more stashes currently available.
    """
    bucket = storage_client.bucket(CHANGE_BUCKET)
    blob = bucket.blob(f"{change_id}.json")
    blob.upload_from_string(data, content_type="application/json")
    log(blob.public_url)
    data_json: JsonDict = json.loads(data)
    return data_json["next_change_id"], bool(data_json["stashes"])


def get_change(change_id: str) -> JsonDict:
    """Return the JSON for the given change."""
    bucket = storage_client.bucket(CHANGE_BUCKET)
    blob = bucket.blob(f"{change_id}.json")
    data = blob.download_as_text()
    return json.loads(data)


def store_stash(stash_id: str, fp: io.BytesIO) -> str:
    """Return the public URL of the stash as JSONL."""
    bucket = storage_client.bucket(STASH_BUCKET)
    blob = bucket.blob(f"{stash_id}.jsonl")
    fp.seek(0)
    blob.upload_from_file(fp, content_type="application/jsonlines")
    log(blob.public_url)
    return blob.public_url
