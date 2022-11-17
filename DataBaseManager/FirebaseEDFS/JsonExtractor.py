import json


class JsonExtractor:
    def __init__(self):
        pass

    def to_json(self, filename):
        return json.load(open(filename))
