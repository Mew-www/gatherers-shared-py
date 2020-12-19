import json


class MeterFile:
    def __init__(self):
        self.size = 0

    def write(self, string):
        self.size += len(string)


def measure_json_size(obj, *args, **kwargs):
    """Returns length of resulting json string"""
    mf = MeterFile()
    json.dump(obj, mf, *args, **kwargs)
    return mf.size
