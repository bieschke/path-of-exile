from common import JsonDict
from gcs_logging import log
from google.cloud import storage
import io
import json
from typing import Tuple

BUCKET = "path-of-exile-public-stashes"


def store_change(change_id: str, data: str) -> Tuple[str, bool]:
    """Store the JSON for the given change.

    Return the next change id and a boolean indicating whether or not
    there are more stashes currently available.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(f"change/{change_id}.json")
    blob.upload_from_string(data, content_type="application/json")
    log(blob.public_url)
    data_json: JsonDict = json.loads(data)
    return data_json["next_change_id"], bool(data_json["stashes"])


def get_change(change_id: str) -> JsonDict:
    """Return the JSON for the given change."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(f"change/{change_id}.json")
    data = blob.download_as_text()
    return json.loads(data)


def store_stash(stash_id: str, fp: io.BytesIO) -> str:
    """Return the public URL of the stash as JSONL."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(f"stash/{stash_id}.jsonl")
    fp.seek(0)
    blob.upload_from_file(fp, content_type="application/jsonlines")
    log(blob.public_url)
    return blob.public_url
