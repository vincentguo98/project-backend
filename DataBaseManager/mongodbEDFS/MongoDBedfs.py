import string
import pymongo
import math


class MongoDBEDFS:
    def __init__(self):
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        client.drop_database('mydatabase')
        db = client['mydatabase']

        self.directory_collection = db["directoryCollection"]
        collection0 = db["partitionCollection0"]
        collection1 = db["partitionCollection1"]
        collection2 = db["partitionCollection2"]
        collection3 = db["partitionCollection3"]
        collection4 = db["partitionCollection4"]
        collection5 = db["partitionCollection5"]
        collection6 = db["partitionCollection6"]
        collection7 = db["partitionCollection7"]
        collection8 = db["partitionCollection8"]

        self.partition_collections = [collection0, collection1, collection2, collection3,
                                      collection4, collection5, collection6, collection7, collection8]

    def mkdir(self, path):
        if not path.startswith("/"):
            raise Exception("input directory not starts with /")
        if path.endswith("/"):
            raise Exception("input directory ends with /")

        last_index_slash = path.rfind("/")
        parent_path = path[0:last_index_slash + 1]
        parent_directory = self.directory_collection.find_one(
            {"path": parent_path, "type": "directory"})
        if not parent_directory:
            if parent_path != "/":
                raise Exception("parent directory " +
                                parent_path + " not created")
            else:
                root_path = {
                    "path": "/",
                    "type": "directory",
                    "children": []
                }
                self.directory_collection.insert_one(root_path)

        new_path = {
            "path": path + "/",
            "type": "directory",
            "children": []
        }
        parent_path_dict = self.directory_collection.find_one(
            {"path": parent_path})
        parent_path_dict.get("children").append({
            "path": path + "/",
            "type": "directory",
        })
        self.directory_collection.insert_one(new_path)
        self.directory_collection.update_one(
            {"path": parent_path}, {"$set": parent_path_dict})

    def ls(self, path):
        if not path.startswith("/"):
            raise Exception("input directory not starts with /")
        if path.endswith("/"):
            raise Exception("input directory ends with /")

        query_result = self.directory_collection.find_one(
            {"path": path + "/", "type": "directory"})
        if not query_result:
            print("There are no such directory name " + path)
        child = query_result['children']
        for i in child:
            name = i['path']
            print(name)

    def cat(self, path):
        query_result = self.directory_collection.find_one(
            {"type": "file", "path": path})
        if not query_result:
            raise Exception("There are no such file " + path)
        for partition_metadata in query_result['partition_metadatas']:
            partition_num = partition_metadata["partition_num"]
            partition_name = partition_metadata["partition_name"]
            file = self.partition_collections[partition_num].find_one(
                {"partition_name": partition_name})
            for line in file["data"]:
                print(line.strip())

    def rm(self, path):
        query_result = self.directory_collection.find_one(
            {"type": "file", "path": path})
        if not query_result:
            print("There are no such file/directory " + path)
        self.directory_collection.delete_one({"path": path})
        last_slash_index = path.rfind("/")
        parent_path = path[0:last_slash_index + 1]
        self.directory_collection.update_one({"path": parent_path}, {
            "$pull": {"children": {"path": path}}})
        for partition_metadata in query_result['partition_metadatas']:
            partition_num = partition_metadata["partition_num"]
            partition_name = partition_metadata["partition_name"]
            self.partition_collections[partition_num].delete_one(
                {"partition_name": partition_name})

        print("file deleted.")

    def put(self, file_name, path, k):
        if k >= 10:
            raise Exception("The partition number is larger than 10")

        if not path.endswith("/"):
            path += "/"

        # if the file exists in database, raise exception
        query_result = self.directory_collection.find_one(
            {"path": file_name}, {"type": "file"})
        if query_result:
            raise Exception("The file already exists")

        # if directory not exists, raise exception
        directory = self.directory_collection.find_one(
            {"path": path, "type": "directory"})
        if not directory:
            if path != "/":
                raise Exception("parent directory " +
                                path + " not created yet")
            else:
                root_path = {
                    "path": "/",
                    "type": "directory",
                    "children": []
                }
                self.directory_collection.insert_one(root_path)

        data = []
        with open(file_name) as file:
            for line in file:
                data.append(line)
        batch_size = math.ceil(len(data) / k)
        partition_metadatas = []
        loc = 0
        si = 0
        while loc < len(data):
            new_partition = {
                "partition_name": path + file_name + "#partition" + str(si),
                "type": "partition",
                "data": data[loc: (len(data) if loc + batch_size > len(data) else loc + batch_size)]
            }
            partition_metadata = {
                "partition_name": path + file_name + "#partition" + str(si),
                "partition_num": si,
            }
            partition_metadatas.append(partition_metadata)
            self.partition_collections[si].insert_one(new_partition)
            loc += batch_size + 1
            si += 1
        new_file = {
            "path": path + file_name,
            "type": "file",
            "partition_metadatas": partition_metadatas
        }
        self.directory_collection.insert_one(new_file)

        parent_path_dict = self.directory_collection.find_one(
            {"path": path, "type": "directory"})
        parent_path_dict.get("children").append({
            "path": path + file_name,
            "type": "file",
        })
        self.directory_collection.update_one(
            {"path": path}, {"$set": parent_path_dict})
        print("file " + file_name + " loaded.")

    def getPartitionLocations(self, path):
        query_result = self.directory_collection.find_one(
            {"type": "file", "path": path})
        if not query_result:
            raise Exception("There is no such file " + path)
        print("Locations are")
        for partition_metadata in query_result['partition_metadatas']:
            partition_num = partition_metadata["partition_num"]
            partition_name = partition_metadata["partition_name"]
            print("partition collection number: " + str(partition_num))
            print("partition name: " + partition_name)

    def readPartition(self, path, partition_num):
        query_result = self.directory_collection.find_one(
            {"type": "file", "path": path})
        if not query_result:
            raise Exception("There is no such file " + path)
        for partition_metadata in query_result['partition_metadatas']:
            if partition_num == partition_metadata["partition_num"]:
                partition_name = partition_metadata["partition_name"]
                file = self.partition_collections[partition_num].find_one(
                    {"partition_name": partition_name})
                print("Printing file name:" + path +
                      "partition number:" + str(partition_num))
                for line in file["data"]:
                    print(line.strip())
                return
        raise Exception(
            "There is no such partition number " + str(partition_num))


if __name__ == "__main__":
    # connect to Mongodb and Create db & Collection

    try:
        md = MongoDBEDFS()
        md.put("cars.json", "/", 5)
        md.put("cars.csv", "/", 5)
        md.cat("/cars.json")
        md.cat("/cars.csv")

        md.mkdir("/user")
        md.mkdir("/user/John")
        md.mkdir("/user/Mary")
        md.ls("/user")
        md.put("cars.json", "/user/John", 5)
        md.cat("/user/John/cars.json")
        md.rm("/user/John/cars.json")
        md.getPartitionLocations("/cars.json")
        md.readPartition("/cars.csv", 3)

    except Exception as e:
        print(e)
