from typing import Union
from collections import namedtuple


RecordTuple = namedtuple("RecordTuple", ["data", "identifying_fields", "last_updated"])


class Record:
    """
    PAST NOTE: Initial implementation was to subclass MutableMapping,
        but this created ad-hoc limitation for the read-only properties as data keys,
        so it was refactored as a simple dict-wrapper.

    Dictionary-wrapping class with

        additional read-only attributes
            .identifying_fields: ["key1", "key2"]
            .last_updated: 15123123123 epoch timestamp in seconds
            .data {} dictionary containing identifying fields and any other fields

        and custom comparison magic methods (__eq__, __ne__)
            comparison checks the values of identifying fields (__init__ sorted to ensure consistency).

    Usage:
    record = Record(arbitrary_dict_with_identifying_fields, identifying_fields=["some_external_id"], timestamp=epoch)
    record.data
    record.identifying_fields
    record.last_updated
    """
    def __init__(self, data: dict, identifying_fields: list, last_updated: Union[int, float]):
        if not identifying_fields:
            raise ValueError("Minimum one identifying field is required")
        for field_name in identifying_fields:
            if field_name not in data:
                raise ValueError(f"Identifying field '{field_name}' is missing from data")
        self.__data = data
        self.__identifying_fields = sorted(identifying_fields)
        self.__last_updated = int(last_updated)

    @property
    def data(self):
        return self.__data

    @property
    def identifying_fields(self):
        return self.__identifying_fields

    @property
    def last_updated(self):
        return self.__last_updated

    def __str__(self):
        return "Record: " + ",".join([f"{k}={self.__data[k]}" for k in self.__identifying_fields])

    def to_dict(self):
        return {"data": self.data, "identifying_fields": self.identifying_fields, "last_updated": self.last_updated}

    # custom instance comparison

    def __identifying_values(self):
        return tuple(self.__data[k] for k in self.__identifying_fields)

    def __hash__(self):
        return hash(self.__identifying_values())

    def __eq__(self, other):
        if isinstance(other, Record):
            return self.__identifying_values() == other.__identifying_values()
        return NotImplemented

    def __ne__(self, other):
        return not self.__eq__(other)
