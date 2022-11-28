from flask import Flask, jsonify, request
from DataBaseManager.EDFSClient import EDFSClient
from flask_cors import CORS
from util import transform_mysql, transform_firebase, getColumnsByFilename

app = Flask(__name__)
CORS(app)
edfs_client = EDFSClient()

@app.route('/api/ls', methods=['GET'])
def ls():
    database = request.args.get('database')
    path = request.args.get('path')
    try:
        files = edfs_client.ls(database, path)
        files = map(lambda x: x.replace("/", ""), files)
    except:
        files = None
    if files is None:
        files = []
    else:
        files = [{"id": name, "name": name, "isDir": True if name.find(".") < 0 else False}
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
    return outputContent(database, path, data)


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
    success = edfs_client.put(database, filename, path, int(partition))
    return jsonify({"success": success})


@app.route('/api/getPartitionLocations', methods=['GET'])
def getPartitionLocations():
    database = request.args.get('database')
    path = request.args.get('path')
    data = edfs_client.getPartitionLocations(database, path)
    response = {}
    if data is None:
        response["success"] = False
    else:
        response["success"] = True
        response["locations"] = data
    return jsonify(response)


@app.route('/api/readPartition', methods=['GET'])
def readPartition():
    database = request.args.get('database')
    path = request.args.get('path')
    partition = request.args.get('partition')
    data = edfs_client.readPartition(database, path, int(partition))
    if data is None:
        data = []
    return outputContent(database, path, data)

@app.route('/api/search')
def search():
    database = request.args.get("database")
    path = request.args.get("path")
    lte = request.args.get("lte")
    gte = request.args.get("gte")
    selectField = request.args.get("selectField")
    whereField = request.args.get("whereField")
    search_results = edfs_client.search(database, path, selectField, whereField, int(lte), int(gte))
    search_results["columns"] = getColumnsByFilename(path.split("/")[-1])
    return search_results


@app.route('/api/analytic')
def analytic():
    database = request.args.get("database")
    path = request.args.get("path")
    lte = request.args.get("lte")
    gte = request.args.get("gte")
    whereField = request.args.get("whereField")
    groupByField = request.args.get("groupByField")
    count_results = edfs_client.count(database, path, whereField, int(lte), int(gte), groupByField)
    count_results["columns"] = getColumnsByFilename(path.split("/")[-1])
    return count_results



@app.route('/test/firebase', methods=['GET'])
def test():
    # print(edfs_client.ls("firebase", "/"))

    # edfs_client.mkdir("firebase", "/a")
    # edfs_client.mkdir("firebase", "/a/d")
    # edfs_client.mkdir("firebase", "/a/b")
    # edfs_client.put("firebase", "california_vaccination.csv", "/a", 3)
    search_res = edfs_client.search("firebase", "/a/california_vaccination.csv", "Cases", "Cases", 1000, 200)
    count_res = edfs_client.count("firebase", "/a/california_vaccination.csv",
                             "Cases", 1000, 200, "CITY")
    return jsonify(search_res)

    # return
    # print(edfs_client.ls("firebase", "/a"))
    # edfs_client.readPartition("firebase", "/a/b/california_vaccination.csv", 1)
    # print(edfs_client.getPartitionLocations("firebase", "/a/california_vaccination.csv"))
    # edfs_client.rm("firebase", "/a/california_vaccination.csv")
    # print(edfs_client.ls("firebase", "/a"))
    # return jsonify({"ok": 200})


@app.route('/test/mysql', methods=['GET'])
def testMysql():
    print(edfs_client.ls("mysql", "/"))
    # edfs_client.mkdir("mysql", "/a")
    # edfs_client.mkdir("mysql", "/a/d")
    # edfs_client.mkdir("mysql", "/a/b")
    # edfs_client.put("mysql", "california_vaccination.csv", "/", 3)
    # print(edfs_client.ls("mysql", "/a"))
    # print(edfs_client.cat("mysql", "/california_vaccination.csv"))
    # edfs_client.readPartition("mysql", "/a/california_vaccination.csv", 1)
    # print(edfs_client.getPartitionLocations("mysql", "/a/california_vaccination.csv"))
    # edfs_client.rm("mysql", "/a/california_vaccination.csv")
    # print(edfs_client.ls("mysql", "/a"))
    return jsonify({"ok": 200})

def outputContent(database, path, data):
    if database == "mysql":
        print(data)
        return jsonify(transform_mysql(path.split("/")[-1], data))
    if database == "firebase":
        tes = transform_firebase(path.split("/")[-1], data)
        print(tes)
        return jsonify(tes)
    return jsonify({"data": data})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
