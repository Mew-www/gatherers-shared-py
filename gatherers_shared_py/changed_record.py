from .record import Record
from typing import TypedDict, Dict


class ChangedRecord(TypedDict):
    record: Record  # the record that has changed (fields) content
    added: Dict  # added fields
    changed: Dict  # changed fields
    removed: Dict  # removed fields
