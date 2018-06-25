#!encoding:utf-8
# ####################################################################
#   File name:  operation
#   Author:李明志      Version:1.00        Date: 2017-5-10
#
#   Description:   用于每日数据接收完毕后对数据的离线处理,计算来访周期和预测数据的产生
#   Function List:
#         data_get()
#         data_operate()
#         data_process()
#         data_analysis()
#         visit_cycle()
# #####################################################################
import sys
import os
sys.path.append(os.path.abspath(".."))
import datetime
import re
from multiprocessing import Pool
import perdict_info
from DataBase.DataBase import mysqlConnect, mysqlExecute


time = datetime.datetime.now()
time = (time + datetime.timedelta(days=0))


def data_get():
    """ Function:       data_get()
        Description:    查询inshop_time中当日flag为0的数据,每个mac地址查询一次
        Input:          无
        Return:         :return :查询到的数据的个数和全部的数据
        Others:         无
    """
    time1 = datetime.datetime.strftime(time, "%Y-%m-%d 00:00:00")
    time2 = datetime.datetime.strftime(time, "%Y-%m-%d 23:00:00")
    sql = "select distinct phone_mac from inshop_time where flag=0 and time between '%s' and '%s'" % (
        time1, time2)
    result = mysqlExecute(mysqlConnect(), sql)
    macs = result.fetchall()
    mac = list(macs)
    print len(mac)
    return (len(mac), mac)


def data_operate(mac):
    """ Function:       data_operate()
        Description:    计算每日的来访周期并存入数据库
        Input:          mac地址
        Return:         :return :无
        Others:根据传入的mac地址来判断data_set中是否有该mac,若没有则将mac和时间插入data_set,
        若有则根据新老时间来计算来访周期,删除旧数据,将mac,时间和来访周期存入表中.
    """
    pattern = re.compile(r'\d+')
    time1 = time.strftime("%Y-%m-%d 00:00:00")
    print len(mac)
    for i in mac:
        # and exists(select * from data_set where phone_mac='%s')
        sql = "select time from data_set where phone_mac ='%s'" % (list(i)[0])
        result = mysqlExecute(mysqlConnect(), sql)
        res = result.fetchone()
        if res is not None:
            circle_time = str(time - res[0])
            match = pattern.match(circle_time)
            circle_time = match.group()
            sql = "delete from data_set where phone_mac='%s'" % list(i)[0]
            mysqlExecute(mysqlConnect(), sql)
            sql = "insert into data_set(phone_mac,time,circle_time) values('%s','%s','%s')" % (
                list(i)[0], time1, circle_time)
            mysqlExecute(mysqlConnect(), sql)
        else:
            sql = "insert into data_set(phone_mac,time) values('%s','%s')" % (
                list(i)[0], time1)
            mysqlExecute(mysqlConnect(), sql)


def data_process():

    """ Function:       data_process()
        Description:    多进程来调用data_operate()
        Input:          无
        Return:         :return :无
        Others:接收data_get()函数的返回值,根据得到的数据长度将数据分为4部分,
        启动四个进程来执行data_operate()函数,将四部分数据分别分给四个进程来执行.
    """
    start = datetime.datetime.now()
    print 'start transport'
    p = Pool()
    all, mac = data_get()
    num = all / 4
    for i in range(3):
        p.apply_async(data_operate, args=(mac[num * i:num * (i + 1)],))
    p.apply_async(data_operate, args=(mac[num * 3:all],))
    p.close()
    p.join()
    sql = "delete from hive_data.inshop_time where flag=1"
    mysqlExecute(mysqlConnect(), sql)
    sql = "update hive_data.inshop_time set flag=3"
    mysqlExecute(mysqlConnect(), sql)
    data_analysis()
    end = datetime.datetime.now()
    print 'All subprocesses done,take time:' + str(end - start)[0:-7]


def data_analysis():
    """ Function:       data_analysis()
        Description:    获取每日的客流量,入店量,天气和当天是否是节假日并存入数据库,用于数据预测
        Input:          无
        Return:         :return :无
        Others:客流量和入店量根据data和inshop_time表来查询.天气,节假日和每日8:00到22:00温度调用predict_info
        中的get_weather,get_day_type和get_temperature来获取,将每小时的气温和每小时的入店量,客流量转成字符串,
        数据间用#间隔,数据存入data_predict表中
    """
    time1 = datetime.datetime.strftime(time, "%Y-%m-%d 08:00:00")
    time2 = datetime.datetime.strftime(time, "%Y-%m-%d 23:00:00")
    time3 = datetime.datetime.strftime(time, "%Y-%m-%d 00:00:00")
    time_day = datetime.datetime.strftime(time, "%Y%m%d")
    #time=(time + datetime.timedelta(days=+1)).strftime("%Y%m%d")
    sql = "select count(distinct phone_mac) from hive_data.data where time between '%s' and '%s'" % (
        time1, time2)
    res = mysqlExecute(mysqlConnect(), sql)
    num = res.fetchone()
    all_num = num[0]
    sql = "select count(distinct phone_mac) from hive_data.inshop_time where time between '%s' and '%s'" % (
        time1, time2)
    res = mysqlExecute(mysqlConnect(), sql)
    inshop_num = res.fetchone()[0]
    sql = "select count(phone_mac)  from hive_data.inshop_time where time between '%s' and '%s'" % (
        time1, time2)
    res = mysqlExecute(mysqlConnect(), sql)
    all_inshop_num = res.fetchone()[0]
    sql = "select lat,lon from hive_data.data limit 1"
    res = mysqlExecute(mysqlConnect(), sql)
    location = res.fetchone()
    print location
    weather = perdict_info.get_weather(location[0], location[1], 'today')
    # weather=predict_info.get_weather('40.8','116.1','today')
    holiday = perdict_info.get_day_type(time_day)
    temperature = perdict_info.get_temperature(
        location[0], location[1], 'today')
    if(temperature == "fail"):
        s = (time + datetime.timedelta(days=-1)).strftime("%Y-%m-%d 00:00:00")
        e = (time + datetime.timedelta(days=-1)).strftime("%Y-%m-%d 23:00:00")
        sql = "select hour_temperature from hive_data.data_predict where time between '%s' and '%s'" % (
            s, e)
        result = mysqlExecute(mysqlConnect(), sql)
        result = result.fetchone()[0]
        temperature_str = result
    # temperature=predict_info.get_temperature('40.8','116.1')
    else:
        temperature_str = ""
        pattern = '\d+'
        for i in range(15):
            temperature1 = re.findall(pattern, temperature[i])
            temperature_str += (temperature1[0] + "#")
        temperature_str = temperature_str[:-1]
    time_s1 = datetime.datetime.strftime(time, "%Y-%m-%d 08:00:00")
    time_e1 = datetime.datetime.strftime(time, "%Y-%m-%d 08:00:00")
    count = ""
    count1 = ""
    for i in range(8, 23):
        time_s = time_s1
        time_e = time_e1
        time_s = time_s.replace(time_s[-8:-6], str(i))
        time_e = time_e.replace(time_e[-8:-6], str(i + 1))
        sql = "select count(distinct phone_mac) from hive_data.data where time between '%s' and '%s'" % (
            time_s, time_e)
        result = mysqlExecute(mysqlConnect(), sql)
        result = result.fetchone()[0]
        count += (str(result) + "#")
    count = count[:-1]
    for i in range(8, 23):
        time_s = time_s1
        time_e = time_e1
        time_s = time_s.replace(time_s[-8:-6], str(i))
        time_e = time_e.replace(time_e[-8:-6], str(i + 1))
        sql = "select count(distinct phone_mac) from hive_data.inshop_time where time between '%s' and '%s'" % (
            time_s, time_e)
        result = mysqlExecute(mysqlConnect(), sql)
        result = result.fetchone()[0]
        count1 += (str(result) + "#")
    count1 = count1[:-1]
    sql = "delete from hive_data.data_predict where time='%s'" % time3
    mysqlExecute(mysqlConnect(), sql)
    sql = "insert into hive_data.data_predict values('%s','%s','%s','%s','%s','%s'," \
          "'%s','%s','%s')" % (all_num, inshop_num, weather, holiday, time3,'', temperature_str, count, count1)

    mysqlExecute(mysqlConnect(), sql)


if __name__ == '__main__':
    data_analysis()
