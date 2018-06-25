#!encoding:utf-8
# ####################################################################
#   File name:  olddata_query
#   Author:李明志      Version:1.00        Date: 2017-5-10
#   Description:   计算历史数据并存储。
#   Function List:
#         hiveConnect()
#         hiveExecute()
#         inshop_time()
#         passenger()
#         circle_time()
# #####################################################################
import sys
import os
sys.path.append(os.path.abspath(".."))
from DataBase.DataBase import hiveConnect, hiveExecute,mysqlExecute,mysqlConnect,writeLog
import json
from datetime import datetime


def inshop_time():
    """ Function:       inshop_time()
            Description:    根据时间查询驻店时长。
            Input:          查询时间范围。
            Return:         包含查询结果的json串。
            Others:         无。
        """
    sql = "Select time,inshop_8,inshop_12,inshop_15,inshop_18,inshop_21 from history_count"
    cur = mysqlExecute(mysqlConnect(), sql)
    data = list()
    for day in cur._rows:
        data.append([datetime.strptime(str(day[0])[:11] + "08:00:00","%Y-%m-%d %H:%M:%S"), day[1]])
        data.append([datetime.strptime(str(day[0])[:11] + "12:00:00","%Y-%m-%d %H:%M:%S"), day[2]])
        data.append([datetime.strptime(str(day[0])[:11] + "15:00:00","%Y-%m-%d %H:%M:%S"), day[3]])
        data.append([datetime.strptime(str(day[0])[:11] + "18:00:00","%Y-%m-%d %H:%M:%S"), day[4]])
        data.append([datetime.strptime(str(day[0])[:11] + "21:00:00","%Y-%m-%d %H:%M:%S"), day[5]])
    return json.dumps(data,cls=MyEncoder)


class MyEncoder(json.JSONEncoder):
  def default(self, obj):
      if isinstance(obj, datetime):
          return obj.strftime('%Y-%m-%d %H:%M:%S')
      elif isinstance(obj, datetime.date):
          return obj.strftime('%Y-%m-%d')
      else:
          return json.JSONEncoder.default(self, obj)


def passenger():
    """
    Function:       passenger()
    Description:    根据时间查询客流量,入店量和入店率。
    Input:          查询时间范围和查询内容的标记。
    Return:         包含查询结果的json串。
    Others:         无。
            """
    sql = "Select time,time_8,time_12,time_15,time_18,time_21 from history_count"
    error = False
    try:
        cur = hiveExecute(hiveConnect(),sql)
    except Exception:
        error = True
        try:
            cur = mysqlExecute(mysqlConnect(), sql)
        except Exception:
            writeLog('无法从hive或者mysql数据库获取历史数据！','[ERROR]')
            return  None
    data = list()
    if error:
        temp = cur._rows
    else:
        temp = cur.fetch()
    for day in temp:
        data.append([datetime.strptime(str(day[0])[:11] + "08:00:00","%Y-%m-%d %H:%M:%S"), day[1]])
        data.append([datetime.strptime(str(day[0])[:11] + "12:00:00","%Y-%m-%d %H:%M:%S"), day[2]])
        data.append([datetime.strptime(str(day[0])[:11] + "15:00:00","%Y-%m-%d %H:%M:%S"), day[3]])
        data.append([datetime.strptime(str(day[0])[:11] + "18:00:00","%Y-%m-%d %H:%M:%S"), day[4]])
        data.append([datetime.strptime(str(day[0])[:11] + "21:00:00","%Y-%m-%d %H:%M:%S"), day[5]])
    return json.dumps(data,cls=MyEncoder)


def circle_time(time1,time2):
    """ Function:       circle_time()
        Description:    :param: datetime: 根据时间查询来访周期。
        Input:          :return json: 查询时间范围。
        Return:         包含查询结果的json串。
        Others:         无。
    """
    sql = "select circle_time from data_set where time between '%s' and '%s'" % (time1, time2)
    res = hiveExecute(hiveConnect(), sql)
    result = res.fetch()
    result1 = [str(i[0]) for i in result]
    data = json.dumps(result1)
    return data


def get_suggest(flag):
    """ Function:       circle_time()
        Description:    获取预测信息并返回数据到网页。
        Input:          :param: str: 查询时间范围。
        Return:         :return: json: 包含查询结果的json串。
        Others:         无。
       """
    time = datetime.now()
    time = time.strftime("%Y-%m-%d 00:00:00")
    sql="select * from data_predict where time='%s'"%(time)
    result=mysqlExecute(mysqlConnect(),sql)
    result=result.fetchone()

    if flag=='day':
        try:
            return list(result[0:4])
        except Exception:
            return [-1,-1,-1]
    elif flag=='hour':
        hour_number=[]
        hour_number_inshop=[]
        try:
            for i in list(result)[-2].split('#'):
                hour_number.append(int(i))
            for i in list(result)[-1].split('#'):
                hour_number_inshop.append(int(i))
            data={'a':hour_number,'b':hour_number_inshop}
            data=json.dumps(data)
            return data
        except Exception :
            return [-1,-1,-1]
    elif flag=='week':
        week_number=[]
        week_number_inshop=[]
        try:
            for i in list(result)[-4].split('#'):
                week_number.append(int(i))
            for i in list(result)[-3].split('#'):
                week_number_inshop.append(int(i))
            data={'a':week_number,'b':week_number_inshop}
            data=json.dumps(data)
            return data
        except Exception :
            return [-1,-1,-1]


if __name__ == '__main__':
    print get_suggest('hour')
