from common import JsonDict
from google.cloud import storage
import json
from typing import Tuple

BUCKET_PUBLIC_STASHES = "path-of-exile-public-stashes"


def store_public_stashes(change_id: str, data: str) -> Tuple[str, bool]:
    """Store a chunk of public stashes.

    Return the next change id and a boolean indicating whether or not
    there are more stashes currently available."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_PUBLIC_STASHES)
    blob = bucket.blob(change_id)
    blob.upload_from_string(data)
    log(f"store_public_stashes({change_id})")
    data_json: JsonDict = json.loads(data)
    return data_json["next_change_id"], bool(data_json["stashes"])


def get_public_stashes(change_id: str) -> JsonDict:
    """Return the JSON structure for the given change id."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_PUBLIC_STASHES)
    blob = bucket.blob(change_id)
    data = blob.download_as_text()
    return json.loads(data)
