from .FirebaseEDFS.FileSystemClient import FileSystemClient
import os

class EDFSClient:
    def __init__(self):
        self.database = {}
        self.register("firebase", FileSystemClient())

    def register(self, database_type, client):
        self.database[database_type] = client

    def mkdir(self, database_type, path):
        # Expect return True if update success, False if fail
        self.database[database_type].mkdir(path)
        return self.ls(database_type, path)

    def ls(self, database_type, path):
        # Expect return [filename/directory1, filename/directory2 ]
        return self.database[database_type].ls(path)

    def rm(self, database_type, path):
        self.database[database_type].rm(path)

    def put(self, database_type, filename, path, partition):
        abs_path = os.path.abspath("./datasets")
        full_filename = os.path.join(abs_path, filename)
        self.database[database_type].put(full_filename, path, partition)

    def getPartitionLocations(self, database_type, filename):
        return self.database[database_type].getPartitionLocations(filename)

    def readPartition(self, database_type, filename, partition):
        return self.database[database_type].readPartition(filename, partition)

    def cat(self, database_type ,filename):
        return self.database[database_type].cat(filename)