#! encoding:UTF-8
# ####################################################################
#   Copyright (C), team BugKiller
#   File name:  DataGatheringServer
#   Author:李政      Version:3.05        Date: 2017-06-12
#
#   Description:    离线计算相关任务，计算每日平均时段的入店量，客流
#   量，对过去时间的客流量进行分析统计的程序。
#   Function List:
#         dataImport()
#         dataProcess()
# #####################################################################

from DataGathering import DataImport
from DataBase import DataBase
import time


def dataImport():
    """ Function:       dataImport()
            Description:    调用数据导入函数，将mysql中的数据导入到hive
            Input:          无
            Return:         none
            Others:         none
    """
   	print "等待导入到hive数据！"
	fp1 = open('1','a+')
        DataImport.addImport()
        sql = 'SELECT count(*) FROM hive_data.data WHERE time > "{}" and time < "{}"'.format(
            getTime(delay=24 * 3600) + "23:59:59", getTime() + "23:59:59")
        cur = DataBase.hiveExecute(DataBase.hiveConnect(), sql)
        sql1 = 'SELECT count(*) FROM hive_data.data WHERE time > "{}" and time < "{}"'.format(
            DataImport.getTime1(delay=24 * 3600), DataImport.getTime1())
	count = cur.fetch()[0][0]
        if count == 0:
            print '数据导入出错,Mysql数据将不会清除，请检查错误'
	    fp1.write('数据导入出错,Mysql数据将不会清除，请检查错误\n')
        else:

             sql1 = 'truncate table hive_data.data'
             sql2 = 'truncate table hive_data.inshop_time'
             sql3 = 'delete from hive_data where time < "{}"'.format(DataImport.getTime1(delay=15 * 24 * 3600))
             print sql3

             DataBase.mysqlExecute(DataBase.mysqlConnect(), sql1)
             DataBase.mysqlExecute(DataBase.mysqlConnect(), sql2)

             with open('1','a+') as fp :
		fp.write( 'mysql数据库数据已经清除!'+str(DataImport.getTime1()+'\n'))

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
        time.strftime('%Y-%m-%d',time.localtime(time.time() -delay +advance)))


def dataProcess():
    """ Function:       dataProcess()
            Description:    计算一天中平均五个特征点的入店量和客流量并存入数据库
            Input:          无
            Return:         none
            Others:         none
    """


    print "五点取样中！"

    sql = 'select count(distinct phone_mac) from hive_data.data where time>="{}" and time <="{}"'.format(
        getTime() + " 08:00:00", getTime() + " 08:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    time_8 = cur._rows[0][0]

    sql = 'select count(distinct phone_mac) from hive_data.data where time>="{}" and time <="{}"'.format(
        getTime() + " 08:00:00",getTime() + " 08:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    time_12 = cur._rows[0][0]

    sql = 'select count(distinct phone_mac) from hive_data.data where time>="{}" and time <="{}"'.format(
        getTime() + " 12:00:00", getTime() + " 12:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    time_15 = cur._rows[0][0]

    sql = 'select count(distinct phone_mac) from hive_data.data where time>="{}" and time <="{}"'.format(
        getTime() + " 15:00:00", getTime() + " 15:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    time_18 = cur._rows[0][0]

    sql = 'select count(distinct phone_mac) from hive_data.data where time>="{}" and time <="{}"'.format(
        getTime() + " 21:00:00", getTime() + " 21:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    time_21 = cur._rows[0][0]

    ##########################################################################

    sql = 'select count(distinct phone_mac) from hive_data.inshop_time where time>="{}" and time <="{}"'.format(
        getTime() + " 08:00:00", getTime() + " 08:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    inshop_8 = cur._rows[0][0]

    sql = 'select count(distinct phone_mac) from hive_data.inshop_time where time>="{}" and time <="{}"'.format(
        getTime() + " 08:00:00", getTime() + " 08:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    inshop_12 = cur._rows[0][0]

    sql = 'select count(distinct phone_mac) from hive_data.inshop_time where time>="{}" and time <="{}"'.format(
        getTime() + " 12:00:00",getTime() + " 12:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    inshop_15 = cur._rows[0][0]

    sql = 'select count(distinct phone_mac) from hive_data.inshop_time where time>="{}" and time <="{}"'.format(
        getTime() + " 15:00:00", getTime() + " 15:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    inshop_18 = cur._rows[0][0]

    sql = 'select count(distinct phone_mac) from hive_data.inshop_time where time>="{}" and time <="{}"'.format(
       getTime() + " 21:00:00", getTime() + " 21:10:05")
    cur = DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
    inshop_21 = cur._rows[0][0]
    if time_8 == 0 or time_12 == 0 or inshop_18 == 0 or inshop_12  == 0:
    	return 
	print " shu ju wei ling"
    sql = "INSERT into hive_data.history_count(time,time_8,time_12,time_15,time_18,time_21,inshop_8,inshop_12,inshop_15,inshop_18,inshop_21 ) VALUES('{}',{},{},{},{},{},{},{},{},{},{})".format(
        getTime(), time_8, time_12, time_15, time_18, time_21, inshop_8, inshop_12, inshop_15, inshop_18, inshop_21)

    DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)

