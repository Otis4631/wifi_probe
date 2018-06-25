#! encoding:UTF-8
# ####################################################################
#   Copyright (C), team BugKiller
#   File name:  DataImport
#   Author:李政      Version:1.00        Date: 2017-04-14
#
#   Description:    数据导入模块，用于将mysql数据库中一天的数据导入到hive数据库对应日期分区下存储。
#   Function List:
#         allImport()
#         getTime()
#         addImport()
# #####################################################################

import sys
import os
sys.path.append(os.path.abspath(".."))
print 111
import os
import time
from DataBase import DataBase


# 全量导入
def allImport():
    """ Function:       allImport()
        Description:    全量导入函数，用于第一次或者hive数据库中的数据遭受损害时将mysql中所有的数据导入到hive数据库。
        Input:          无。
        Return:         无。
        Others:         需安装apache sqoop工具。
    """
    sqoopCmd = "sqoop import -m 1 --connect jdbc:mysql://localhost:3306/hive_data --username root --password test.py --table data --hive-import --hive-database hive_data --create-hive-table --hive-table data"
    os.system(sqoopCmd)

# 增量导入
def getTime(delay=0, advance=0):
    """ Function:       getTime()
        Description:    获取当前日期的函数。
        Input:          :param delay: 用于调整得到所需时间，非0则返回数据在当前时间之前，单位为秒。
                        :type delay int.

                        :param advance: 用于调整得到所需时间，非0则返回数据在当前时间之后，单位为秒。
                        :type advance int.


        Return:         :return 返回当前时间，格式为 YYYY-MM-DD HH:MM:SS。
                        :rtype str.
        Others:         需安装apache sqoop工具。
    """
    return str(
        time.strftime('%Y-%m-%d_%H-%M-%S',time.localtime(time.time() -delay +advance)))

def getTime1(delay=0, advance=0):
    """ Function:       getTime()
        Description:    获取当前日期的函数。
        Input:          :param delay: 用于调整得到所需时间，非0则返回数据在当前时间之前，单位为秒。
                        :type delay int.

                        :param advance: 用于调整得到所需时间，非0则返回数据在当前时间之后，单位为秒。
                        :type advance int.


        Return:         :return 返回当前时间，格式为 YYYY-MM-DD HH:MM:SS。
                        :rtype str.
        Others:         需安装apache sqoop工具。
    """
    return str(
        time.strftime('%Y-%m-%d %H-%M-%S',time.localtime(time.time() -delay +advance)))


def addImport():

    """ Function:       addImport()
        Description:    增量导入函数，用于每天将数据库中的数据增量导入到hive数据库对应日期下的分区。
        Input:          无。
        Return:         无。
        Others:         需安装apache sqoop工具，需hadoop用户创建目录权限。
    """
    err = False
    time = getTime()
    sqoopCmd = '''
        /root/sqoop/bin/sqoop import --connect jdbc:mysql://211.64.28.102:3306/hive_data --username root --password lizheng1997  --table data   --split-by id -m 1 --target-dir /temp/hive/{}
                '''.format(time)
    hiveCmd = '''
        /root/hive/bin/hive -e "ALTER TABLE hive_data.data ADD PARTITION(partition_name='{}') location '/temp/hive/{}/';"
            '''.format(time, time)


    chmodCmd = "/root/hadoop/bin/hadoop fs -chmod -R 777 /temp/"  # 导入hive可能出现权限不足，需对临时目录降权。

    sqoopCmd1 = '''
            /root/sqoop/bin/sqoop import --connect jdbc:mysql://211.64.28.102:3306/hive_data --username root --password lizheng1997  --table inshop_time   --split-by id -m 1 --target-dir /temp/hive/inshop/{}
                    '''.format(time)
    hiveCmd1 = '''
            /root/hive/bin/hive -e "ALTER TABLE hive_data.inshop_time ADD PARTITION(partition_name='{}') location '/temp/hive/inshop/{}/';"
                '''.format(time, time)

    sqoopCmd2 = '''
            /root/sqoop/bin/sqoop import --connect jdbc:mysql://211.64.28.102:3306/hive_data --username root --password lizheng1997 --table history_count   --split-by id -m 1 --target-dir /temp/hive/history/{}
                    '''.format(time)
    hiveCmd2 = '''
            /root/hive/bin/hive -e "load data  inpath '/temp/hive/history/{}/part-m-00000' into table hive_data.history_count;"
                '''.format(time)


    sqoopCmd3 = '''
            /root/sqoop/bin/sqoop import --connect jdbc:mysql://localhost:3306/hive_data --username root --password lizheng1997 --table data_set   --split-by id -m 1 --target-dir /temp/hive/set/{}
                    '''.format(time)
    hiveCmd3 = '''
            /root/hive/bin/hive -e "load data  inpath '/temp/hive/set/{}/part-m-00000' into table hive_data.data_set;"
                '''.format(time)

    try:
        os.system(chmodCmd)
        os.system(sqoopCmd)
        os.system(hiveCmd)
       	os.system(sqoopCmd1)
       	os.system(hiveCmd1)

    except Exception as error:
        DataBase.writeLog('数据导入出错' + error, '[ERROR]')
        error = True
        print error
        print "from DataImport.py"
    if not err:
        sql = " SELECT count(*) from hive_data.data "
        count = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)._rows[0][0]
        DataBase.writeLog('共向Hive数据库导入' + str(count) + '条数据')
        print '共向Hive数据库导入' + str(count) + '条数据'


if __name__ == "__main__":
    addImport()
