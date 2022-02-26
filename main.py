__doc__ = """Google Cloud Function main() entrypoints."""

import gcs_storage
from public_stashes import public_stashes
from store_stashes import store_stashes


def path_of_exile_subscribe(event, context):
    """Event fired periodically through Scheduler and Pub/Sub to
    retrieve data from the Path of Exile API."""
    change_id = gcs_storage.get_next_change_id()
    next_change_id = public_stashes(change_id, 0.0)
    gcs_storage.set_next_change_id(next_change_id)
    log(f"next change {next_change_id}")


def public_stashes_finalize(event, context):
    """Triggered by a finalize to a Cloud Storage bucket.
    Finalizes occur every time a file is created/overwritten.

    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    filename = event["name"]
    change_id = filename.removesuffix(".json")
    count = store_stashes(change_id)
    log(f"store_stashes({change_id}) -> {count}")
