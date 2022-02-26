__doc__ = """Google Cloud Function main() entrypoints."""

from write_stash_items import store_stashes


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
