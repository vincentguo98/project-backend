import sqlalchemy
import pymysql

pymysql.install_as_MySQLdb()
from pathlib import PurePosixPath
import pandas as pd
import numpy as np


class mysqlEDFSClient:
    def __init__(self):
        try:

            self.conn = pymysql.connect(host="localhost",
                                        port=3306,
                                        user="root",
                                        database="project")
            self.cursor = self.conn.cursor()

            # password = urllib.parse.quote_plus("1djdgQL@")  # '123%40456'
            password = ""
            self.write_conn = sqlalchemy.create_engine(f"mysql+mysqldb://root:{password}@localhost/project")
        except Exception as e:
            print('cant connect to mysql database')
            self.conn.rollback()
            self.cursor.close()
            self.conn.close()

    def close_conn(self):
        self.cursor.close()
        self.conn.close()

    def getDirectoryId(self, dir_list, flag=0):
        parent = 1  # root id is set to be 1
        for i in range(1, len(dir_list) - flag):
            sql = 'select i.* from file_structure s inner join file_info i on i.id = s.child where s.parent = %s;'
            self.cursor.execute(sql, [parent])
            result = self.cursor.fetchall()
            for row in result:
                if row[1] == dir_list[i] and row[2] == 'directory':
                    parent = row[0]
                    break
            else:
                return -1  # can't find intermediate directory or intermediate directory is not directory, report error
        return parent

    def getFileId(self, dir_list):
        parent = self.getDirectoryId(dir_list, 1)
        if parent == -1: return -1
        sql = 'select i.* from file_structure s inner join file_info i on i.id = s.child where s.parent = %s;'
        self.cursor.execute(sql, [parent])
        result = self.cursor.fetchall()
        file_id = -1
        for row in result:
            if row[1] == dir_list[-1] and row[2] == 'file':
                file_id = row[0]
        return file_id

    def mkdir(self, path):
        dir_list = path.split('/')
        if dir_list == ['', '']:
            return False
        parent = self.getDirectoryId(dir_list, 1)
        if parent == -1: return False
        try:
            sql = 'call mkdir(%s, %s);'
            self.cursor.execute(sql, [dir_list[-1], parent])
            self.conn.commit()
            return True
        except Exception as e:
            print(e)
            return False

    def ls(self, path):
        dir_list = path.split('/')
        if dir_list == ['', '']:
            dir_list.remove('')
        parent = self.getDirectoryId(dir_list)
        if parent == -1: return None

        sql = 'select i.* from file_structure s inner join file_info i on i.id = s.child where s.parent = %s;'
        self.cursor.execute(sql, [parent])
        result = self.cursor.fetchall()
        return [row[1] for row in result]

    def rm(self, path):
        dir_list = path.split('/')
        if dir_list == ['', '']:
            return False
        file_id = self.getFileId(dir_list)
        if file_id == -1: return False
        table_name = self.concatTableName(dir_list)
        partition_list = self.getPartitionLocations(path)
        try:
            sql = 'call rm(%s);'
            self.cursor.execute(sql, [file_id])
            self.conn.commit()

            sql = f'drop table if exists {table_name};'
            self.cursor.execute(sql)
            for partition in partition_list:
                sql = f'drop table if exists {partition};'
                self.cursor.execute(sql)
            self.conn.commit()
            return True
        except Exception as e:
            print(e)
            return False

    def put(self, filename, path, partition):
        full_filename = filename
        filename = filename.split("/")[-1]
        # check if the path is valid
        dir_list = path.split('/')
        if dir_list == ['', '']:
            dir_list.remove('')
        parent = self.getDirectoryId(dir_list)
        if parent == -1: return False
        sql = 'select i.* from file_structure s inner join file_info i on i.id = s.child where s.parent = %s;'
        self.cursor.execute(sql, [parent])
        result = self.cursor.fetchall()
        for row in result:
            if row[1] == filename and row[2] == 'file':
                return False

        # read file
        if PurePosixPath(filename).suffix == '.csv':
            df = pd.read_csv(full_filename, keep_default_na=False)
        elif PurePosixPath(filename).suffix == '.json':
            df = pd.read_json(full_filename)
        else:
            return False
        df.insert(loc=0, column='row_num', value=np.arange(len(df)))
        table_name = PurePosixPath(filename).stem.lower()
        if len(dir_list) > 1:
            table_name = '_'.join(dir_list[1:]) + '_' + table_name

        # write to tables
        try:
            # write data
            for i in range(partition):
                df[df['row_num'] % partition == i].to_sql(table_name + '_' + str(i), self.write_conn,
                                                          if_exists='replace',
                                                          index=False)

            partition_info = pd.DataFrame([table_name + '_' + str(i) for i in range(partition)], columns=['location'])
            partition_info.to_sql(table_name, self.write_conn, if_exists='replace', index=False)

            # write metadata
            sql = 'call put(%s, %s);'
            self.cursor.execute(sql, [filename, parent])
            self.conn.commit()
            return True
        except Exception as e:
            print(e)
            return False

    def concatTableName(self, dir_list):
        table_name = PurePosixPath(dir_list[-1]).stem.lower()
        if len(dir_list) > 2:
            table_name = '_'.join(dir_list[1:-1]) + '_' + table_name
        return table_name

    def getPartitionLocations(self, path):
        dir_list = path.split('/')
        if dir_list == ['', '']:
            dir_list.remove('')
        file_id = self.getFileId(dir_list)
        if file_id == -1: return None

        table_name = self.concatTableName(dir_list)
        partition_info = pd.read_sql(f'select * from {table_name}', self.write_conn)
        return list(partition_info['location'])

    def getMaxPartition(self, path):
        location = self.getPartitionLocations(path)
        max_partition = 0
        for loc in location:
            index = int(loc.split('_')[-1])
            if index > max_partition:
                max_partition = index
        return max_partition

    def readPartition(self, path, partition):
        max_partition = self.getMaxPartition(path)
        if max_partition < partition:
            return None
        dir_list = path.split('/')
        if dir_list == ['', '']:
            dir_list.remove('')
        table_name = self.concatTableName(dir_list) + '_' + str(partition)
        df = pd.read_sql(f'select * from {table_name}', self.write_conn)
        return df.to_dict(orient='records')

    def cat(self, path):
        location = self.getPartitionLocations(path)
        df = pd.concat([pd.read_sql(f'select * from {table_name}', self.write_conn) for table_name in location], axis=0,
                       ignore_index=True)
        df.sort_values(by='row_num', inplace=True, ignore_index=True)
        return df.to_dict(orient='records')


if __name__ == '__main__':

    try:
        sc = mysqlEDFSClient()
        sc.mkdir("/test1")
        print('mkdir success')
        sc.mkdir("/test3")
        print('mkdir2 success')
        print(sc.ls('/'))
        sc.put("LA_County_COVID_Testing.csv", "/test3", 5)
        print(sc.getPartitionLocations("/test3/LA_County_COVID_Testing.csv"))
        print(sc.readPartition("/test3/LA_County_COVID_Testing.csv", 2))
        print(sc.cat("/test3/LA_County_COVID_Testing.csv"))
        sc.rm("/test3/LA_County_COVID_Testing.csv")
        print(sc.ls('/test3'))
    except Exception as e:
        print("something wrong")
        # sc.close_conn()
