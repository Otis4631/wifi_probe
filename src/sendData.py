#! encoding: utf-8
# ####################################################################
#   Copyright (C), team BugKiller
#   File name:  DataGatheringServer
#   Author:李政      Version:1.00        Date: 2017-04-12
#
#   Description:   模拟数据发送程序，自动随机生成顾客模拟进入店铺的场景，此程序也可做测试脚本，在主程序中可设定并发数。
#   Function List:
#         send()
#         getMac()
#         ct()
#         createData()
#         createWifiInfo()
#         wifiProbe()
# #####################################################################

import threading
import time
import random
import datetime
import json
import requests
import multiprocessing

WIFI_AP_SSID = (u'hello1', u'hello2')

Maclist = []


def getMac():
    """ Function:       getMac()
        Description:    随机生成mac地址
        Input:          无
        Return:         无
        Others:         mac地址字符串
    """
	Maclist = []
	for i in range(1,7):
    		RANDSTR = "".join(random.sample("0123456789abcdef",2))
    		Maclist.append(RANDSTR)
		RANDMAC = ":".join(Maclist)
	return RANDMAC

MAC_TABLE =list()
for i in range(1000):
        MAC_TABLE.append(str(getMac()))



def ct():
    """ Function:       ct()
        Description:    随机顾客手机信息
        Input:          无
        Return:         顾客手机信息 tpye:dict
        Others:         无
    """
    return {
        u'mac': random.choice(MAC_TABLE),
        u'rssi': random.uniform(-100, 0),
        u'range': random.randrange(0, 300),
        u'ts': random.choice(WIFI_AP_SSID),
        u'tc': random.choice(('Y', 'N')),
        u'ds': random.choice(('Y', 'N')),
        u'tmc': u'tmc',
        u'essid': u'essid',
        u'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }


def createData(wifi_info,):
    """ Function:       createData()
        Description:    调用顾客信息生成函数，在设定并发基数的上随机生成500-1000条顾客信息。
        Input:          wifi_info:模拟探针发送的数据
        Return:         无
        Others:         无
    """
    wifi_info[u'data'] = []
    for i in range(random.randrange(500,1000)):
        wifi_info[u'data'].append(ct())


def createWifiInfo(wifi_info, id):
    """ Function:       createWifiInfo()
        Description:    创建WIFI探针发送的数据
        Input:          wifi_info:模拟探针发送的数据，id:wifi探针ID
        Return:         dict数据
        Others:         无
    """
    wifi_info[u'id'] = id
    wifi_info[u'rate'] = 1
    wifi_info[u'mmac'] = u'mmac'
    wifi_info[u'wssid'] = u'wssid'
    wifi_info[u'wmac'] = u'wmac'
    wifi_info[u'addr'] = u'addr'
    wifi_info[u'lat'] = u'30.748093'
    wifi_info[u'lon'] = u'103.973083'
    createData(wifi_info)
    return wifi_info


def wifiProbe(host):
    """ Function:       wifiProbe()
        Description:    将模拟的数据发往指定主机地址
        Input:          host：主机地址。
        Return:         无
        Others:         无
    """
	def create(id):
		wifi_info = dict()
		flag = 0

		a = createWifiInfo(wifi_info, id)
		jsonEncode = json.dumps(wifi_info)
		try:
			# print wifi_info['id']
			requests.post('http://' + host + '/post', data={'data': jsonEncode})
		except Exception as e:
				if flag == 0:
					print u'等待开启数据采集模块...'
					flag = 1
		if flag == 0:
			print u'send'
			pass

	create(random.randrange(0,3))
		


def send(occurs=4, host='211.64.28.102'):
    """ Function:       send()
        Description:    堵塞式主程序，设定发送并发数的基数，并发数计算方式：
                        Min:occurs * 500; Max:occurs * 1000
                        默认并发数为2000~4000，发送间隔为2s
        Input:          host：主机地址。
        Return:         无
        Others:         无
    """

    while True:
    	p = multiprocessing.Pool(occurs)
        for i in range(occurs):
        	p.apply_async(wifiProbe, (host + ':80',))
        p.close()
        p.join()
        time.sleep(2)


probe_info = {}


if __name__ == "__main__":
    send()
