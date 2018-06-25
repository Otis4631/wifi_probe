#!encoding:utf-8
# ####################################################################
#   File name:  operation
#   Author:李明志      Version:1.00        Date: 2017-5-10
#
#   Description:   用于实时数据的存储和处理,并用于数据展示请求的处理
#   Function List:
#         passenger()
#         save_data()
#         visit_cycle()
# #####################################################################
import sys
import os
sys.path.append(os.path.abspath(".."))
import datetime
import re
import json
from DataBase.DataBase import mysqlExecute, mysqlConnect, hiveConnect, hiveExecute


def passenger(command, time):
    """ Function:       passenger()
                  Description:    用于查询数据库,计算客流量,入店量和入店率.
                  Input:          command选择查询客流量或入店量或入店率,时间。
                  Return:         :return :.json,包含两部分,查询结果和输入的时间
                  Others:         无
              """
    if command == "total_number":
        a = time
        sql = "select count(distinct phone_mac) from data where time between '%s' and '%s'" % (
            str(time + datetime.timedelta(seconds=-3)), str(a))
        result = mysqlExecute(mysqlConnect(), sql)
        s = result.fetchone()[0]
        data = {"a": s, "b": str(time)}
        data = json.dumps(data)
        return data

    elif command == "inshop_number":
        a = time
        sql = "select count(distinct phone_mac) from inshop_time where flag=1 and time between '%s' and '%s'" % (
            str(time + datetime.timedelta(seconds=-10)), str(a))
        result = mysqlExecute(mysqlConnect(), sql)
        s = result.fetchone()[0]
        data = {"a": s, "b": str(time)}
        data = json.dumps(data)
	print data
        return data
    elif command == "inshop_rate":
        a = time
        sql = "select count(distinct phone_mac) from data where time between '%s' and '%s'" % (
            str(time + datetime.timedelta(seconds=-6)), str(a))
        result = mysqlExecute(mysqlConnect(), sql)
        total_number = float(result.fetchone()[0])
        if total_number == 0:
            data = {"a": 0, "b": str(time)}
            data = json.dumps(data)
            return data
        sql = "select count(distinct phone_mac) from data where phone_range<=10 and time between '%s' and '%s'" % (
            str(time + datetime.timedelta(seconds=-6)), str(a))
        result = mysqlExecute(mysqlConnect(), sql)
        inshop_number = float(result.fetchone()[0])
        s = inshop_number / total_number
        data = {"a": s, "b": str(time)}
        data = json.dumps(data)
        return data


def time_stay(jsonDecode, inshop_range, probe_id):
    """ Function:       time_stay()
                          Description:    接收实时数据,计算驻店时长并存储,10秒以下设为1,10-30秒设为2,30-60设为3,60以上为4.
                          Input:          接收到的json。
                          Return:         :return :无
                          Others:         计算方法:inshop_time表存储所有进入过店铺的mac,当接收一个新mac,
                          查询inshop_time表是否有flag为1的该mac的记录,如果表中没有该mac并且该mac在店铺内,将mac和time存入表,flag设为1,
                          如果表中有该mac并且该mac在店铺外,查询表中该mac对应的时间,并与接收到的mac时间做差,将查询到的mac更新,flag设为0,
                          并将对应的时间差的代号存入.
                      """
    inshop_range = int(inshop_range)
    pattern1 = re.compile(r'0:00:0' + '\d')
    pattern2 = re.compile(r'0:00:[12]' + '\d')
    pattern3 = re.compile(r'0:00:[345]' + '\d')
    sql = "select count(*) from inshop_time where phone_mac='%s' and flag=1" % jsonDecode['data']['mac']
    res = mysqlExecute(mysqlConnect(), sql)
    result = res.fetchone()[0]
   
    if result == 0 and jsonDecode['data']['range'] <= inshop_range:
        sql = "insert into inshop_time (phone_mac,time,flag,p_range) values('{}','{}',1,{})".format(
            jsonDecode['data']['mac'], jsonDecode['data']['time'], inshop_range)
        mysqlExecute(mysqlConnect(), sql)
    elif result == 1 and jsonDecode['data']['range'] > inshop_range :
        sql = "select time from inshop_time where flag=1 and phone_mac='%s'" % jsonDecode[
            'data']['mac']
        try:
            cur = mysqlExecute(mysqlConnect(), sql)
            old_time = cur.fetchone()[0]
            recent = datetime.datetime.strptime(
                str(jsonDecode['data']['time']), "%Y-%m-%d %H:%M:%S")
            circle = str(recent - old_time)
            if pattern1.match(circle):
                stage = 1
            elif pattern2.match(circle):
                stage = 2
            elif pattern3.match(circle):
                stage = 3
            else:
                stage = 4
            sql = "update inshop_time set stay_time='%s',flag=0, time_diff='%s' where flag=1 and phone_mac='%s'" % (
                str(stage), str(circle),jsonDecode['data']['mac'])
            mysqlExecute(mysqlConnect(), sql)
        except Exception as e:
            print e


def time_stay_query(time):
    """ Function:       time_stay_query()
                              Description:    根据时间查询驻店时长并按比例返回
                              Input:          时间。
                              Return:         :return :json,包含四个时间的比例
                              Others:         无
                          """
    time1 = datetime.datetime.strftime(time, "%Y-%m-%d 08:00:00")
    a = datetime.datetime.strftime(time, "%Y-%m-%d %H:%M:%S")
    sql = "select stay_time from inshop_time where flag=0 and time between '" + \
        time1 + "' and '" + a + "'"
    result = mysqlExecute(mysqlConnect(), sql)
    s = result.fetchall()
    s = list(s)
    time1 = 0
    time2 = 0
    time3 = 0
    time4 = 0
    for i in s:
        if '1' == list(i)[0]:
            time1 += 1
        elif '2' == list(i)[0]:
            time2 += 1
        elif '3' == list(i)[0]:
            time3 += 1
        else:
            time4 += 1
    time = time4 + time3 + time2 + time1
    if time == 0:
        time = 1
        time1 = 1
    data = {"a": time1, "b": time2, "c": time3, "d": time4}
    data = json.dumps(data)
    return data


def passenger_type():
    time = datetime.datetime.now()
    time1 = time.strftime("%Y-%m-%d 08:00:00")
    sql = "select circle_time from data_set where  phone_mac in(select distinct phone_mac " \
        "from inshop_time where time between '%s' and '%s')" % (time1, time)
    result = mysqlExecute(mysqlConnect(), sql)
    old = float(len(result.fetchall()))
    sql = "select distinct phone_mac from data where time between '%s' and '%s'" % (
        time1, time)
    result = mysqlExecute(mysqlConnect(), sql)
    all_number = float(len(result.fetchall()))
    new = all_number - old
    data = {'new': new, 'old': old, 'all': all_number}
    data = json.dumps(data)
    return data


def deep_jump():
    sql = "select stay_time from inshop_time where flag=0"
    result = mysqlExecute(mysqlConnect(), sql)
    result = result.fetchall()
    count_deep = 0
    count_all = 0
    count_jump = 0
    for i in list(result):
        if '1' == list(i)[0] or '2' == list(i)[0]:
            count_jump += 1
        count_all += 1
    count_deep = count_all - count_jump

    data = {'deep': count_deep, 'jump': count_jump, 'all': count_all}
    data = json.dumps(data)
    return data


def active_rate():
    sql = "select circle_time from data_set"
    result = mysqlExecute(mysqlConnect(), sql)
    result = result.fetchall()
    count1 = count2 = count3 = count4 = 0
    for i in list(result):
        if 0 < int(list(i)[0]) <= 7:
            count1 += 1
        elif int(list(i)[0]) in range(8, 15):
            count2 += 1
        elif int(list(i)[0]) in range(15, 30):
            count3 += 1
        elif int(list(i)[0]) >= 30:
            count4 += 1
    data = {'high': count1, 'middle': count2, 'low': count3, 'sleep': count4}
    data = json.dumps(data)
    return data


def visit_circle():
    sql = "select circle_time from data_set"
    result = mysqlExecute(mysqlConnect(), sql)
    all_num = result.fetchall()
    count1 = count2 = count3 = count4 = count5 = count6 = count7 = count8 = count9 = count10 = count11 = 0
    for i in list(all_num):
        if int(list(i)[0]) <= 3:
            count1 += 1
        elif int(list(i)[0]) in range(4, 7):
            count2 += 1
        elif int(list(i)[0]) in range(7, 10):
            count3 += 1
        elif int(list(i)[0]) in range(10, 13):
            count4 += 1
        elif int(list(i)[0]) in range(13, 16):
            count5 += 1
        elif int(list(i)[0]) in range(16, 19):
            count6 += 1
        elif int(list(i)[0]) in range(19, 22):
            count7 += 1
        elif int(list(i)[0]) in range(22, 25):
            count8 += 1
        elif int(list(i)[0]) in range(25, 28):
            count9 += 1
        elif int(list(i)[0]) in range(28, 31):
            count10 += 1
        else:
            count11 += 1
    data = {
        'a': count1,
        'b': count2,
        'c': count3,
        'd': count4,
        'e': count5,
        'f': count6,
        'g': count7,
        'h': count8,
        'i': count9,
        'j': count10,
        'k': count11}
    data = json.dumps(data)
    return data