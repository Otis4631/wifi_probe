#! encoding:UTF-8
# ####################################################################
#   Copyright (C), team BugKiller
#   File name:  DataGatheringServer
#   Author:李政      Version:1.00        Date: 2017-04-12
#
#   Description:    数据采集模块，使用Flask框架采用多线程处理数据源并发，接受Json格式的数据，将数据处理完毕存入mysql数据库。
#   Function List:
#         post()
#         start()
# #####################################################################
import sys
import os
sys.path.append(os.path.abspath(".."))
import datetime
import json
import time
from flask import Flask, request
from DataBase import DataBase
try:
	from DataGathering import info
except Exception as e:
	import info
	print e
from operation import operation
ERROR = False
app = Flask(__name__)
BOOL = {'Y': 1, 'N': 0}  # 用于转换WIFI探针中的Bool类型到mysql中可识别的bool类型。
dataKeys = set()
keys = set()
dataKeys.add('ds')      # 手机是否睡眠
dataKeys.add('tc')      # 是否与路由器相连
dataKeys.add('ts')      # 目标ssid，手机链接的WIFI的ssid
dataKeys.add('tmc')     # 目标设备的mac，手机链接WIFI的mac
dataKeys.add('essid')   # 手机连接的ssid
keys.add('wssid')
keys.add('wmac')
keys.add('addr')


@app.route('/', methods=['GET', 'POST'])
def welcome():
    """ Function:       welcome()
        Description:    用于显示欢迎信息。
        Input:          无。
        Return:         :return str: 成功链接提示.
                        :rtype str.
        Others:         无。
    """
    return "welcome"

    
@app.route('/post', methods=['POST'])
def post():
    """ Function:       post()
        Description:    用于接收来自数据源的Json数据，只接受POST方法。
        Input:          无。
        Return:         :return str: 成功链接提示.
                        :rtype str.
        Others:         无。
    """
    global ERROR
    jsonEncode = request.form.get('data')
    jsonDecode = json.loads(jsonEncode)
    #对接受的json数据进行清洗
    ############################################
    for key in keys:
        if key not in jsonDecode:
            jsonDecode[key] = u'NULL'
    if not jsonDecode['lon']:
        jsonDecode['lon'] = -1
    if not jsonDecode['lat']:
        jsonDecode['lat'] = -1
    for key in keys:
        if key not in jsonDecode:
            jsonDecode[key] = u'NULL'
    for item in jsonDecode['data']:
        for key in dataKeys:
            if key not in item:
                item[key] = 'NULL'
        if item['ds'] != u'NULL':
            item['ds'] = BOOL[item['ds']]
        else:
            item['ds'] = -1
        if item['tc'] != u'NULL':
            item['tc'] = BOOL[item['tc']]
        else:
            item['tc'] = -1
        item['time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        dicts = {}
        dicts['data'] = {}
        dicts['data']['range'] = item['range']
        dicts['data']['mac'] = item['mac']
        dicts['data']['time'] = item['time']
        dicts['data']['id'] = jsonDecode['id']
    ###############################################
        try:
            sql = """
                     INSERT INTO data
                             (probe_id,probe_mac,rate,wssid,wmac,time,lat,lon,
                             addr,phone_mac,phone_rssi,phone_range,phone_tmc, tc,ds,essid,ts)
                       VALUES('{}','{}',{},'{}','{}','{}',{},{},'{}','{}',{},{},'{}',{},{},"{}","{}")
                     """.format(
                jsonDecode['id'],jsonDecode['mmac'],jsonDecode['rate'],jsonDecode['wssid'],jsonDecode['wmac'],
                item['time'],jsonDecode['lat'],jsonDecode['lon'],jsonDecode['addr'],item['mac'],
                item['rssi'],item['range'],item['tmc'],item['tc'],item['ds'],item['essid'],item['ts'])

        except Exception as error:
            print error
        try:
            DataBase.mysqlExecute(DataBase.mysqlConnect(), sql)
            pass

        except Exception as error:
            ERROR = True
            DataBase.writeLog("数据插入异常：" + str(error), '[ERROR]')
        inshop_range = info.get_data('range')
        device_id = info.get_data('id')
        operation.time_stay(dicts, inshop_range,device_id)

    if not ERROR:
        print str(len(jsonDecode['data'])) + "条数据已存入数据库"
    else:
        print '出现错误'
    return 'welcome'


def start():
    """ Function:           start()
            Description:    启动函数。
            Input:          无。
            Return:         无。
            Others:         无。
    """
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)


if __name__.__eq__('start'):
    start()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=False, threaded=True)



