import json

class JsonFileSplitter:
    def __init__(self):
        pass

    def to_list_of_json(self, partition, json_data):
        json_data_list = [item for item in json_data]
        unit_length = int(len(json_data_list) / partition)
        result = []
        for i in range(0, partition):
            unit_json = {}
            for key in json_data_list[i * unit_length: min((i + 1) * unit_length, len(json_data_list))]:
                unit_json[key] = json_data[key]
            result.append(unit_json)
        return result

if __name__ == '__main__':
    splitter = JsonFileSplitter()
    data = splitter.to_list_of_json(4)
    print(data)


