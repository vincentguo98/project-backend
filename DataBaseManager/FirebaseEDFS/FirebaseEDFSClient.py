from . import Constants
import requests
import os
import urllib.parse
from .JsonFileSplitter import *
from .CSVExtractor import *
from .JsonExtractor import *


class FireBaseRequester:
    def __init__(self, URL):
        self.url = URL

    def get(self, path):
        abs_path = urllib.parse.urljoin(self.url, path)
        r = requests.get(abs_path)
        return r.json()

    def put(self, path, json_data):
        abs_path = urllib.parse.urljoin(self.url, path)
        return requests.put(abs_path, json.dumps(json_data))

    def patch(self, path, patched_data):
        abs_path = urllib.parse.urljoin(self.url, path)
        return requests.patch(abs_path, json.dumps(patched_data))

    def remove(self, path):
        abs_path = os.path.join(self.url, path)
        return requests.delete(abs_path)


class PathExporter:
    def __init__(self, rootName, dataName):
        self.root = rootName
        self.data = dataName

    def toRootJson(self, path):
        path_list = path.split('/')
        if path_list is None:
            return None
        path_list[0] = self.root
        json_data = {}
        cur = json_data
        for d in path_list:
            cur[d] = {}
            cur['*'] = "*"
            cur = cur[d]
        cur["*"] = "*"
        print(json_data)
        return json_data

    def toRootPath(self, path, full_filename=""):
        if path[0] == '/':
            path = path[1:len(path)]
        return os.path.join(os.path.join(self.root, path), full_filename).replace('.', '-') + ".json"

    def toPartitionedDataPath(self, full_filename, partition):
        if full_filename[0] == '/':
            full_filename = full_filename[1:len(full_filename)]
        return os.path.join(os.path.join(self.data, full_filename.replace('.', '-').replace('/', '-')),
                            str(partition)) + ".json"


class FirebaseEDFSClient:
    def __init__(self):
        self.URL = Constants.URL
        self.root = Constants.ROOT
        self.data = Constants.DATA
        self.client = FireBaseRequester(self.URL)
        self.pathExporter = PathExporter(Constants.ROOT, Constants.DATA)
        self.jsonFileSplitter = JsonFileSplitter()
        self.csvExtractor = CSVExtractor()
        self.jsonExtractor = JsonExtractor()

    def mkdir(self, path):
        self.client.patch("/root" + path + ".json", {'*' :'*'})

    def ls(self, path):
        directory = self.client.get(self.pathExporter.toRootPath(path))
        if isinstance(directory, dict):
            return [x for x in directory.keys() if x[0] != '*']
        return directory

    def rm(self, path):
        dataPath = self.pathExporter.toPartitionedDataPath(path, "")
        self.client.remove(dataPath)
        metaDataPath = self.pathExporter.toRootPath(path)
        self.client.remove(metaDataPath)

    def put(self, filename, path, partition):
        json_data = {}
        partition = int(partition)
        full_path = os.path.join(path, filename)
        if filename.endswith("csv"):
            json_data = self.csvExtractor.to_json(full_path)
        if filename.endswith("json"):
            json_data = self.jsonExtractor.to_json(full_path)
        partitioned_data_list = self.jsonFileSplitter.to_list_of_json(partition, json_data)
        meta_data_path = self.pathExporter.toRootPath(path, filename)
        meta_data = {}
        for i in range(0, partition):
            partitioned_path = self.pathExporter.toPartitionedDataPath(os.path.join(path, filename), i)
            self.client.patch(partitioned_path, partitioned_data_list[i])
            meta_data[i] = urllib.parse.urljoin(self.URL, partitioned_path)
        self.client.patch(meta_data_path, meta_data)

    def getPartitionLocations(self, full_filename):
        path = self.pathExporter.toRootPath(full_filename)
        return self.client.get(path)

    def readPartition(self, full_filename, partition):
        return self.client.get(self.pathExporter.toPartitionedDataPath(full_filename, partition))

    def cat(self, full_filename):
        for data_url in self.ls(full_filename):
            print(self.client.get(data_url))


if __name__ == '__main__':
    fs = FirebaseEDFSClient()
    # fs.mkdir("/parent1/parent2")
    # fs.mkdir("/parent2")
    # fs.put("california_vaccination.csv", "/parent1/parent2", 3)
    # r = fs.getPartitionLocations("/parent1/parent2/california_vaccination.csv")
    # r = fs.readPartition("/parent1/parent2/california_vaccination.csv", 2)
    # fs.cat("/parent1/parent2/california_vaccination.csv")
    # fs.mkdir("/some/path/to/file")
