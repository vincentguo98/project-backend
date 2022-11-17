import json

import pandas as pd

class CSVExtractor():
    def __init__(self):
        pass

    def to_json(self,filename):
        csv = pd.read_csv(filename)
        data = csv.to_json(orient='records', lines=True)
        json_data = {}
        for index, item in  enumerate(data.strip().split("\n")):
            json_data[str(index)] = json.loads(item)
        return json_data
