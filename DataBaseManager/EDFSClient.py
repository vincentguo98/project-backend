from .FirebaseEDFS.FirebaseEDFSClient import FirebaseEDFSClient
from .mysqlEDFS.mysqlEDFSClient import mysqlEDFSClient
import os

class EDFSClient:
    def __init__(self):
        self.database = {}
        # TODO  Please register "mysql" @Yating and "mongoDB" @Wenyuan here"
        self.register("firebase", FirebaseEDFSClient())
        # self.register("sql", mysqlEDFSClient())

    def register(self, database_type, client):
        self.database[database_type] = client

    def mkdir(self, database_type, path):
        # Expect return True if update success, False if fail
        return self.database[database_type].mkdir(path)

    def ls(self, database_type, path):
        # Expect return a list consist of filename or directory name
        # e.g ["vaccination.csv", "dir1", "dir2" ]
        return self.database[database_type].ls(path)

    def rm(self, database_type, path):
        # Expect return True if update success, False if fail
        return self.database[database_type].rm(path)

    def put(self, database_type, filename, path, partition):
        # filename: name of uploaded file without path
        # path: target path in EDFS
        # partition: partition count
        abs_path = os.path.abspath("./datasets")
        filename = os.path.join(abs_path, filename)
        print(filename)
        # Expect return True if update success, False if fail
        return self.database[database_type].put(filename, path, partition)

    def getPartitionLocations(self, database_type, full_filename):
        # filename: full filename in EDFS including path e.g /user/john/xxx.csv
        # return List of partition locations
        # e.g ["https://dsci551-b6052-default-rtdb.firebaseio.com/data/file1.csv", ...]
        return self.database[database_type].getPartitionLocations(full_filename)

    def readPartition(self, database_type, full_filename, partition):
        # filename: full filename in EDFS including path e.g /user/john/xxx.csv
        # partition: partition index
        # return the content of a file partition, string
        return self.database[database_type].readPartition(full_filename, partition)

    def cat(self, database_type, full_filename):
        # filename: full filename in EDFS including path e.g /user/john/xxx.csv
        # return the content of a file , string
        return self.database[database_type].cat(full_filename)
