def getColumns(s):
    print(s)
    res = []
    for field in s.strip().split(","):
        res.append({"title": field, "field": field})
    print(res)
    return res


def getColumnsByFilename(filename):
    f = open(f"./datasets/{filename}")
    res = getColumns(f.readline())
    f.close()
    return res


def transform_firebase(filename, data):
    f = open(f"./datasets/{filename}")
    return {"columns": getColumns(f.readline()), "data": data}


def transform_mysql(filename, data):
    f = open(f"./datasets/{filename}")
    s = f.readline()
    f.close()
    return {"columns": getColumns(s), "data": data}


if __name__ == '__main__':
    transform_mysql("california_vaccination.csv", [])
