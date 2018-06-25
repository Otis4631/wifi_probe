#!coding:utf-8
"""
几种影响因素，一种预测结果
客流量，入店量
"""
import sys
import os
sys.path.append(os.path.abspath(".."))
import numpy as np
from DataBase.DataBase import mysqlExecute,mysqlConnect
import perdict_info
import datetime

def get_data(flag):
    sql = "select lat,lon from hive_data.data limit 1"
    res = mysqlExecute(mysqlConnect(), sql)
    res = res.fetchone()
    #print res
    if flag=="day_number":
        time=datetime.datetime.now()
        time = datetime.datetime.strftime(time, "%Y%m%d")
        weather_predict=perdict_info.get_weather(res[0],res[1],'tomorrow')
        holiday_predict=perdict_info.get_day_type(time)
        sql="select all_number,inshop_number,weather,holiday from data_predict"
        result=mysqlExecute(mysqlConnect(),sql)
        result=result.fetchall()
        result=list(result)
        all_number=[]
        inshop_number=[]
        weather=[]
        holiday=[]
        for i in result:
            all_number.append(int(list(i)[0]))
            inshop_number.append(int(list(i)[1]))
            weather.append(int(list(i)[2]))
            holiday.append(int(list(i)[3]))
        return weather,holiday,weather_predict,holiday_predict,all_number,inshop_number
    elif flag=="hour_number":
        data = perdict_info.get_temperature(res[0], res[1], "tomorrow")
        temperature=[]
        for i in data:
            temperature.append(int(i))
        sql="select hour_temperature,hour_number,hour_number_inshop from data_predict"
        result=mysqlExecute(mysqlConnect(),sql)
        result=list(result.fetchall())
        hour_temperature=[]
        hour_number=[]
        hour_number_inshop=[]
        for i in result[0][0].split('#'):
            hour_temperature.append(int(i))
        for i in result[0][1].split('#'):
            hour_number.append(int(i))
        for i in result[0][2].split('#'):
            hour_number_inshop.append(int(i))
        return hour_temperature,temperature,hour_number,hour_number_inshop
def logsig(x):    #激活函数：1/（1+e^(-x))
    return 1/(1+np.exp(-x))
#预测客流量：天气，节假日，，
def predict(flag):
    if flag=='day_number':
        weather,holiday,weather_predict,holiday_predict,all_number,inshop_number=get_data(flag)
        return bp(weather,holiday,all_number,inshop_number,weather_predict,holiday_predict,'day_number')
    elif flag=='hour_number':
        hour_temperature, temperature, hour_number, hour_number_inshop=get_data(flag)
        return bp(hour_temperature,'',hour_number,hour_number_inshop,temperature,'','hour_number')
    elif flag=='week_number':
        sql = "select lat,lon from hive_data.data limit 1"
        res = mysqlExecute(mysqlConnect(), sql)
        res = res.fetchone()
        weather_num,a,b,c,all_number,inshop_number=get_data('day_number')
        temp_predict,weather_predict=perdict_info.get_weektemp(res[0],res[1])
        sql="select temperature from data_predict"
        res=mysqlExecute(mysqlConnect(),sql)
        res=list(res.fetchall())
        #print res
        temperature=[]
        for i in res:
            temperature.append(list(i)[0])
        temp2=[]
        for i in temperature:
            temp2.append(int(i))
        all_num=[]
        inshop_num=[]

        for i in range(len(temp_predict)):
            alls,inshop= bp(temp2,weather_num,all_number,inshop_number,temp_predict[i],weather_predict[i],'day_number')
            all_num.append(alls)
            inshop_num.append(inshop)
        return all_num,inshop_num,temp_predict[0]
def save_result():
    all_number,inshop_number=predict('day_number')
    hour_number,hour_number_inshop=predict('hour_number')
    week_number,week_number_inshop,temperature=predict('week_number')
    weather, holiday, weather_predict, holiday_predict, all_number1, inshop_number1 = get_data('day_number')
    hour_temperature, temperature, hour_number1, hour_number_inshop1 = get_data('hour_number')
    hour_temperature_str=""
    hour_number_str=""
    hour_number_inshop_str=""
    for i in range(15):
        hour_temperature_str+=(str(temperature[i])+"#")
        hour_number_str+=(str(int(hour_number[i]))+"#")
        hour_number_inshop_str+=(str(int(hour_number_inshop[i]))+"#")
    #print hour_temperature_str[:-1],hour_number_str[:-1],hour_number_inshop_str[:-1]
    week_number_str=""
    week_number_inshop_str=""
    for i in range(7):
        #week_temperature_str+=(str(temperature[i])+"#")
        week_number_str+=(str(int(week_number[i]))+"#")
        week_number_inshop_str+=(str(int(week_number_inshop[i]))+"#")
    print week_number_str,week_number_inshop_str
    
    time = datetime.datetime.now()
    time=time+ datetime.timedelta(days=+1)
    time = datetime.datetime.strftime(time, "%Y-%m-%d 00:00:00")
    sql = "insert into hive_data.data_predict(all_number,inshop_number,weather,holiday,time,temperature,week_temperature,hour_temperature,hour_number,hour_number_inshop,week_number,week_number_inshop) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (all_number,inshop_number,weather_predict,holiday_predict,time,temperature[0],'',hour_temperature_str[:-1],hour_number_str[:-1],hour_number_inshop_str[:-1],week_number_str[:-1],week_number_inshop_str[:-1])
    print sql
    mysqlExecute(mysqlConnect(),sql)

def bp(factor1,factor2,result1,result2,predict1,predict2,flag):
    if flag=='day_number':
        try:
            samplein = np.mat([factor1, factor2])  # 3*20,转成矩阵的形式,影响因素
            sampleinminmax = np.array([samplein.min(axis=1).T.tolist()[0], samplein.max(axis=1).T.tolist()[0]]).transpose()  # 3*2，对应最大值最小值
            sampleout = np.mat([result1, result2])  # 2*20，转矩阵，结果
            sampleoutminmax = np.array([sampleout.min(axis=1).T.tolist()[0], sampleout.max(axis=1).T.tolist()[0]]).transpose()  # 2*2，对应最大值最小值
            # 3*20
            sampleinnorm = (2 * (np.array(samplein.T) - sampleinminmax.transpose()[0]) / (sampleinminmax.transpose()[1] - sampleinminmax.transpose()[0]) - 1).transpose()
            # 2*20
            sampleoutnorm = (2 * (np.array(sampleout.T).astype(float) - sampleoutminmax.transpose()[0]) / (sampleoutminmax.transpose()[1] - sampleoutminmax.transpose()[0]) - 1).transpose()
            # 给输出样本添加噪音,避免过度拟合
            noise = 0.03 * np.random.rand(sampleoutnorm.shape[0], sampleoutnorm.shape[1])
            sampleoutnorm += noise
            maxepochs = 60000  # 最大训练次数
            learnrate = 0.035  # 学习率
            errorfinal = 0.65 * 10 ** (-3)  # 最终的错误允许范围
            samnum = len(factor1)  # 训练样本的个数
            indim = 2  # 输入的因素的个数..
            outdim = 2  # 输出的结果的个数
            hiddenunitnum = 8  # 隐藏神经元个数
            w1 = 0.5 * np.random.rand(hiddenunitnum, indim) - 0.1  # 初始化输入层与隐含层之间的权值
            b1 = 0.5 * np.random.rand(hiddenunitnum, 1) - 0.1  # 初始化输入层与隐含层之间的阈值
            w2 = 0.5 * np.random.rand(outdim, hiddenunitnum) - 0.1  # 初始化输出层与隐含层之间的权值
            b2 = 0.5 * np.random.rand(outdim, 1) - 0.1  # 初始化输出层与隐含层之间的阈值
            errhistory = []
            for i in range(maxepochs):
                hiddenout = logsig((np.dot(w1, sampleinnorm).transpose() + b1.transpose())).transpose()  # 隐含层网络输出
                networkout = (np.dot(w2, hiddenout).transpose() + b2.transpose()).transpose()  # 输出层网络输出
                err = sampleoutnorm - networkout  # 实际输出与网络输出之差
                sse = sum(sum(err ** 2))  # 能量函数（误差平方和）
                errhistory.append(sse)
                if sse < errorfinal:  # 如果误差允许，不再学习
                    break
                # 以下六行是BP网络最核心的程序
                # 他们是权值（阈值）依据能量函数负梯度下降原理所作的每一步动态调整量
                delta2 = err
                delta1 = np.dot(w2.transpose(), delta2) * hiddenout * (1 - hiddenout)
                dw2 = np.dot(delta2, hiddenout.transpose())
                db2 = np.dot(delta2, np.ones((samnum, 1)))
                dw1 = np.dot(delta1, sampleinnorm.transpose())
                db1 = np.dot(delta1, np.ones((samnum, 1)))
                w2 += learnrate * dw2  # 对输出层与隐含层之间的权值和阈值进行修正
                b2 += learnrate * db2
                w1 += learnrate * dw1  # 对输入层与隐含层之间的权值和阈值进行修正
                b1 += learnrate * db1
            predict_1=[]
            predict_1.append(predict1)
            predict_2 = []
            predict_2.append(predict2)
            samplein = np.mat([predict_1, predict_2])
            sampleinnorm = (2 * (np.array(samplein.T) - sampleinminmax.transpose()[0]) / (
            sampleinminmax.transpose()[1] - sampleinminmax.transpose()[0]) - 1).transpose()
            hiddenout = logsig((np.dot(w1, sampleinnorm).transpose() + b1.transpose())).transpose()
            networkout = (np.dot(w2, hiddenout).transpose() + b2.transpose()).transpose()
            diff = sampleoutminmax[:, 1] - sampleoutminmax[:, 0]
            networkout2 = (networkout + 1) / 2
            networkout2[0] = networkout2[0] * diff[0] + sampleoutminmax[0][0]  # 预测的结果
            networkout2[1] = networkout2[1] * diff[1] + sampleoutminmax[1][0]
            return int(networkout2[0]),int(networkout2[1])
        except StandardError, e:
            print e 
	    print "function_bp"
        return result1[-1],result2[-1]
    elif flag=='hour_number':
        try:
            samplein = np.mat([factor1])  # 3*20,转成矩阵的形式,影响因素
            sampleinminmax = np.array([samplein.min(axis=1).T.tolist()[0], samplein.max(axis=1).T.tolist()[0]]).transpose()  # 3*2，对应最大值最小值
            sampleout = np.mat([result1, result2])  # 2*20，转矩阵，结果
            sampleoutminmax = np.array([sampleout.min(axis=1).T.tolist()[0], sampleout.max(axis=1).T.tolist()[0]]).transpose()  # 2*2，对应最大值最小值
            # 3*20
            sampleinnorm = (2 * (np.array(samplein.T) - sampleinminmax.transpose()[0]) / (sampleinminmax.transpose()[1] - sampleinminmax.transpose()[0]) - 1).transpose()
            # 2*20
            sampleoutnorm = (2 * (np.array(sampleout.T).astype(float) - sampleoutminmax.transpose()[0]) / (sampleoutminmax.transpose()[1] - sampleoutminmax.transpose()[0]) - 1).transpose()
            # 给输出样本添加噪音,避免过度拟合
            noise = 0.03 * np.random.rand(sampleoutnorm.shape[0], sampleoutnorm.shape[1])
            sampleoutnorm += noise
            maxepochs = 60000  # 最大训练次数
            learnrate = 0.035  # 学习率
            errorfinal = 0.65 * 10 ** (-3)  # 最终的错误允许范围
            samnum = len(factor1)  # 训练样本的个数
            indim = 1  # 输入的因素的个数..
            outdim = 2  # 输出的结果的个数
            hiddenunitnum = 8  # 隐藏神经元个数
            w1 = 0.5 * np.random.rand(hiddenunitnum, indim) - 0.1  # 初始化输入层与隐含层之间的权值
            b1 = 0.5 * np.random.rand(hiddenunitnum, 1) - 0.1  # 初始化输入层与隐含层之间的阈值
            w2 = 0.5 * np.random.rand(outdim, hiddenunitnum) - 0.1  # 初始化输出层与隐含层之间的权值
            b2 = 0.5 * np.random.rand(outdim, 1) - 0.1  # 初始化输出层与隐含层之间的阈值
            errhistory = []
            for i in range(maxepochs):
                hiddenout = logsig((np.dot(w1, sampleinnorm).transpose() + b1.transpose())).transpose()  # 隐含层网络输出
                networkout = (np.dot(w2, hiddenout).transpose() + b2.transpose()).transpose()  # 输出层网络输出
                err = sampleoutnorm - networkout  # 实际输出与网络输出之差
                sse = sum(sum(err ** 2))  # 能量函数（误差平方和）
                errhistory.append(sse)
                if sse < errorfinal:  # 如果误差允许，不再学习
                    break
                # 以下六行是BP网络最核心的程序
                # 他们是权值（阈值）依据能量函数负梯度下降原理所作的每一步动态调整量
                delta2 = err
                delta1 = np.dot(w2.transpose(), delta2) * hiddenout * (1 - hiddenout)
                dw2 = np.dot(delta2, hiddenout.transpose())
                db2 = np.dot(delta2, np.ones((samnum, 1)))
                dw1 = np.dot(delta1, sampleinnorm.transpose())
                db1 = np.dot(delta1, np.ones((samnum, 1)))
                w2 += learnrate * dw2  # 对输出层与隐含层之间的权值和阈值进行修正
                b2 += learnrate * db2
                w1 += learnrate * dw1  # 对输入层与隐含层之间的权值和阈值进行修正
                b1 += learnrate * db1
            samplein = np.mat([predict1])
            sampleinnorm = (2 * (np.array(samplein.T) - sampleinminmax.transpose()[0]) / (
            sampleinminmax.transpose()[1] - sampleinminmax.transpose()[0]) - 1).transpose()
            hiddenout = logsig((np.dot(w1, sampleinnorm).transpose() + b1.transpose())).transpose()
            networkout = (np.dot(w2, hiddenout).transpose() + b2.transpose()).transpose()
            diff = sampleoutminmax[:, 1] - sampleoutminmax[:, 0]
            networkout2 = (networkout + 1) / 2
            networkout2[0] = networkout2[0] * diff[0] + sampleoutminmax[0][0]  # 预测的结果
            networkout2[1] = networkout2[1] * diff[1] + sampleoutminmax[1][0]
            return networkout2[0],networkout2[1]
        except StandardError, e:
            pass
        return result1,result2