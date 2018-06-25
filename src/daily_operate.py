#! encoding:UTF-8
# ####################################################################
#   Copyright (C), team BugKiller
#   File name:  DataBase
#   Author:李政      Version:3.02        Date: 2017-06-14
#
#   Description:	此程序为每日的定时任务，每隔半个小时执行一次，调用关于hadoop平台的离线计算
#
#   Function List:
#	
# #####################################################################
from operation.predict import save_result
from operation.data_operate import data_process
from hive import dataProcess,dataImport
from DataBase.DataBase import mysqlConnect, mysqlExecute,hiveConnect,hiveExecute
import time
import os


fp = open('hive.py_log','a+')
i = 0

""" Function:       
    Description:    阻塞函数，定时任务，定时调用Hadoop离线计算。
    Input:          无。
    Return:         无。
    Others:         无。
"""
while True:
	sql = """SELECT count(*) from hive_data.data"""
	db = hiveConnect()
	cur = hiveExecute(db,sql)
	print type(cur)
	count = cur.fetch()[0][0]
	with open('hive_count_all','w+') as f:
		f.write(str(count))
		print "总客流量：", count
	if True:
		count = 0
		sql = """SELECT count(*) from hive_data.inshop_time WHERE time < '{}' and time >  '{}'
	""".format(
		str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time() - (7 * 24 * 3600) ))),
		str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time() - (14 * 24 * 3600) ))))
		time.sleep(5)
		cur = hiveExecute(hiveConnect(),sql)
		count = cur.fetch()[0][0]
		print "上周入店量", count
		if count != 0:
			with open('hive_count_all_last_week_in','w+') as f:
				f.write(str(count))
		count = 0
		sql = """SELECT count(*) from hive_data.data WHERE time < '{}' and time >  '{}'
	""".format(
		str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time() - 0 ))),
		str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time() - (7 * 24 * 3600) ))))
		time.sleep(5)
		cur = hiveExecute(hiveConnect(),sql)
		count = cur.fetch()[0][0]
		print "本周客流量",count
		if count != 0:
			with open('hive_count_all_this_week','w+') as f:
				f.write(str(count))
		count = 0
		sql = """SELECT count(*) from hive_data.data WHERE time < '{}' and time >  '{}'
	""".format(
		str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time() - (7 * 24 * 3600) ))),
		str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time() - (14 * 24 * 3600) ))))
		print sql
		time.sleep(5)
		cur = hiveExecute(hiveConnect(),sql)
		count = cur.fetch()[0][0]
		if count  != 0:
			with open('hive_count_all_last_week','w+') as f:
				f.write(str(count))

	sql = """SELECT count(*) from hive_data.inshop_time WHERE time < '{}' AND time > '{}'""".format(
		str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time() ))),
		str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time() - (7 * 24 * 3600) )))
		)
	print sql
	cur = hiveExecute(hiveConnect(),sql)
	count = cur.fetch()[0][0]
	print count
	if count != 0:
		with open('hive_count_all_this_week_in','w+') as f:
			f.write(str(count))
	i = i + 1
	print "start perdict"
	try:	
		save_result()
		data_process()
	except Exception as e:
		print "预测模块异常"
		print e
	try :
		dataProcess()
		dataImport()
	except Exception as e:
		fp.write("数据导入异常\n")
		print "数据导入异常"

	fp.write(str(i) + '\n')
	print "get in sleep"
	time.sleep( 1800)
