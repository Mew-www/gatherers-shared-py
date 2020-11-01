from __future__ import annotations
import time
import datetime
from .record import Record
from .changed_record import ChangedRecord
from typing import List, NamedTuple, TYPE_CHECKING

# Get type hints without importing
# https://github.com/python/mypy/issues/1829#issuecomment-231273766
# https://mypy.readthedocs.io/en/latest/common_issues.html#import
if TYPE_CHECKING:
    from pymongo.collection import Collection


class DiffResults(NamedTuple):
    added: List[Record]
    changed: List[ChangedRecord]
    removed: List[Record]


def diff_and_update_state(
    fresh_records: List[Record],
    collection_state: Collection,
    # These are required for bulk_write, and to force use of pymongo version from actual callee (=no 2 pymongo versions)
    insertOneOp,
    replaceOneOp,
    deleteOneOp,
    retention_period: datetime.timedelta = datetime.timedelta(days=1),
) -> DiffResults:
    # Get former records from StateDB
    former_records = [
        Record(r["data"], r["identifying_fields"], r["last_updated"])
        for r in list(collection_state.find())
    ]

    # Diff for added or refreshed (and possibly changed - subset of refreshed) records
    added_records: List[Record] = []
    changed_records: List[ChangedRecord] = []  # Subset of "refreshed_records"
    # Following is done for performance reasons, with 1000s of op's otherwise CPU will become bottleneck
    bulk_inserts_and_replaces = []
    for fresh_record in fresh_records:

        # Insert added records to cache and remember them via added_records
        if fresh_record not in former_records:
            bulk_inserts_and_replaces.append(insertOneOp(fresh_record.to_dict()))
            added_records.append(fresh_record)

        # Else refresh existing records' timestamp (and possibly fields), and diff for added, changed, or removed fields
        # Timestamp update (for both changed/unchanged) is important because that way we spot "removed" records
        else:
            # Use MongoDB's "nested query" (dot-delimited) syntax for identifier fields
            identifiers_dict = {
                f"data.{k}": fresh_record.data[k]
                for k in fresh_record.identifying_fields
            }
            bulk_inserts_and_replaces.append(
                replaceOneOp(identifiers_dict, fresh_record.to_dict())
            )

            # Diff for added/changed/removed .data fields
            former_matching_record = next(
                filter(lambda r: r == fresh_record, former_records)
            )
            # removed_fields will contain former value
            added_fields, changed_fields, removed_fields = (
                {},
                {},
                {},
            )

            # Check for added fields
            for key in fresh_record.data.keys():
                if key not in former_matching_record.data.keys():
                    added_fields[key] = fresh_record.data[key]

            # Check for removed, and changed fields
            for key, value in former_matching_record.data.items():
                if key not in fresh_record.data.keys():
                    removed_fields[key] = value
                elif value != fresh_record.data[key]:
                    changed_fields[key] = fresh_record.data[key]

            # Remember any records with changed fields
            if added_fields or changed_fields or removed_fields:
                changed_records.append(
                    {
                        "record": fresh_record,
                        "added": added_fields,
                        "changed": changed_fields,
                        "removed": removed_fields,
                    }
                )
    # Persist the inserts and replaces
    collection_state.bulk_write(bulk_inserts_and_replaces, ordered=False)

    # Diff for "removed" records (that haven't been seen in >24 hours)
    removed_records: List[Record] = []
    timedelta_ago = datetime.datetime.fromtimestamp(time.time()) - retention_period
    oldest_permitted_timestamp = timedelta_ago.timestamp()
    # As with inserts & replaces, following is done for performance reasons
    bulk_deletes = []
    for record in former_records:
        # If record was fresh, skip since it's still relevant
        if record in fresh_records:
            continue
        # Remove (>timedelta) non-existent records from cache and remember them via removed_records
        if oldest_permitted_timestamp > record.last_updated:
            # Use MongoDB's "nested query" (dot-delimited) syntax for identifier fields
            identifiers_dict = {
                f"data.{k}": record.data[k] for k in record.identifying_fields
            }
            bulk_deletes.append(deleteOneOp(identifiers_dict))
            removed_records.append(record)
    # Persist the deletes
    collection_state.bulk_write(bulk_deletes, ordered=False)

    return DiffResults(
        added=added_records, changed=changed_records, removed=removed_records,
    )
