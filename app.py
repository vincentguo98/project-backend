from flask import Flask, jsonify, request
from DataBaseManager.EDFSClient import EDFSClient

app = Flask(__name__)
edfs_client = EDFSClient()


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/api/ls', methods=['GET'])
def ls():
    database = request.args.get('database')
    path = request.args.get('path')
    files = edfs_client.ls(database, path)
    if files is None:
        files = {}
    else:
        files = [{"id": name, "name": name, "isDir": True if name.find(".") else False}
                 for name in files]
    return jsonify(files)


@app.route('/api/mkdir', methods=['GET'])
def mkdir():
    database = request.args.get('database')
    path = request.args.get('path')
    success = edfs_client.mkdir(database, path)
    return jsonify({"success": success})


@app.route('/api/cat', methods=['GET'])
def cat():
    database = request.args.get('database')
    path = request.args.get('path')
    data = edfs_client.cat(database, path)
    return jsonify({"data": data})


@app.route('/api/rm', methods=['GET'])
def rm():
    database = request.args.get('database')
    path = request.args.get('path')
    success = edfs_client.rm(database, path)
    return jsonify({"success": success})


@app.route('/api/put', methods=['GET'])
def put():
    database = request.args.get('database')
    path = request.args.get('path')
    filename = request.args.get('filename')
    partition = request.args.get('partition')
    success = edfs_client.put(database, filename, path, partition)
    return jsonify({"success": success})


@app.route('/api/getPartitionLocation', methods=['GET'])
def getPartitionLocations():
    database = request.args.get('database')
    path = request.args.get('path')
    data = edfs_client.getPartitionLocations(database, path)
    response = {}
    if data is None:
        response["success"] = False
    else:
        response["success"] = True
        response["data"] = data
    return jsonify(response)


@app.route('/api/readPartition', methods=['GET'])
def readPartition():
    database = request.args.get('database')
    path = request.args.get('path')
    partition = request.args.get('partition')
    location_list = edfs_client.readPartition(database, path, partition)
    response = {}
    if location_list is None:
        response["success"] = False
    else:
        response["success"] = True
        response["data"] = location_list
    return jsonify(response)


if __name__ == '__main__':
    app.run()
