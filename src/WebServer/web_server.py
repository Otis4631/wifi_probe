#!encoding:utf-8
# ####################################################################
#   File name:  web_server
#   Author:李政      Version:3.04        Date: 2017-6-10
#   Description:  web数据展示服务的核心文件，包含各个网站的访问路由
#   Function List:
#       各个网站的路由函数
# #####################################################################
import sys
import os
sys.path.append(os.path.abspath(".."))
from DataBase.DataBase import mysqlConnect, mysqlExecute, hiveConnect, hiveExecute, getConfigure
import datetime
import json
import history
import time 
import random
from flask import Flask, render_template, request
from operation import operation
app = Flask(__name__)

def getTime(delay=0, advance=0):
    """ Function:       getTime()
        Description:    获取当前日期的函数。
        Input:          :param delay: 用于调整得到所需时间，非0则返回数据在当前时间之前，单位为秒。
                        :type delay int.

                        :param advance: 用于调整得到所需时间，非0则返回数据在当前时间之后，单位为秒。
                        :type advance int.


        Return:         :return 返回当前时间，格式为 YYYY-MM-DD HH:MM:SS。
                        :rtype str.
    """
    return str(
        time.strftime('%Y-%m-%d',time.localtime(time.time() - delay + advance)))


@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
    #主页
def home():
	return render_template('home.html')



@app.route('/predict', methods=['GET', 'POST'])
    #预测页面
def index():
    with open(os.path.abspath('..') + "/hive_count_all", "r") as fp:
                count = int(fp.readline())
    def group(n, sep = ','):
        s = str(abs(n))[::-1]
        groups = []
        i = 0
        while i < len(s):
            groups.append(s[i:i+3])
            i+=3
        retval = sep.join(groups)[::-1]
        if n < 0:
            return '-%s' % retval
        else:
            return retval
    count = group(count)
    data=history.get_suggest('day')
    # data[3]=1
    data.append(count)
    d = {"all":count}
    return render_template('index_data.html',data=data,d=d)



@app.route('/chart', methods=['GET', 'POST'])
    #入店量
def chart():
        with  open(os.path.abspath('..') + "/hive_count_all", "r") as fp:
                count = fp.readline()
        with  open(os.path.abspath('..') + "/hive_count_all_last_week", "r") as fp:
                last_week = fp.readline()
        print last_week
        with  open(os.path.abspath('..') + "/hive_count_all_this_week", "r") as fp:
                this_week = fp.readline()
        with  open(os.path.abspath('..') + "/hive_count_all_this_week_in", "r") as fp:
                this_week_in = fp.readline()
        with  open(os.path.abspath('..') + "/hive_count_all_last_week_in", "r") as fp:
                last_week_in = fp.readline()
        week_data = list()                
        week = list()
        week_in = list()
        for i in range(7):
            try:
                time1 = getTime(i * 24 * 3600)
                sql = """select time_12 from hive_data.history_count WHERE time = "{}"   """.format(time1 + ' 00:00:00')
                try:
                    data =  mysqlExecute(mysqlConnect(),sql)._rows[0][0]
                except Exception as e:
                    data = 0
                week_data.append(data)            
                week.append(time1)
                sql = """select inshop_12 from hive_data.history_count WHERE time = "{}"   """.format(time1 + ' 00:00:00')
                try:
                    data_in =  mysqlExecute(mysqlConnect(),sql)._rows[0][0]
                except Exception as e:
                    data_in = 0
                week_in.append(data_in)
            except Exception as e:
                print e
                week.append(time1)
                week_data.append(105) 
                week_in.append(72)                
        context = {
            'all': count,
            'last_week':last_week,
            'this_week':this_week,
            'week':week,
            'week_data':week_data,
            'week_in':week_in,
            'last_week_in':last_week_in,
            'this_week_in':this_week_in
            }
        print context
        return render_template('total.html',data=context)



BOOL = {'Y': 1, 'N': 0}
# 客流量展示
@app.route('/charts', methods=['GET', 'POST'])
def charts():
    time = datetime.datetime.now()
    data = operation.passenger("total_number", time)
    return data
# 进店量
@app.route('/inshop', methods=['GET', 'POST'])
def inshop():
    return render_template('inshop.html')
# 进店量展示
@app.route('/charts_inshop', methods=['GET', 'POST'])
def charts_inshop():
    time = datetime.datetime.now()
    data = operation.passenger("inshop_number", time)
    return data
# 驻店时长
@app.route('/time_stay_query', methods=['GET', 'POST'])
def f():
    data = operation.time_stay_query(datetime.datetime.now())
    return data
# 驻店时长展示

# 入店率展示
@app.route('/charts_rate', methods=['GET', 'POST'])
def charts_rate():
    time = datetime.datetime.now()
    data = operation.passenger("inshop_rate", time)
    return data
# 历史查询
@app.route('/history_total', methods=['GET', 'POST'])
def history_query1():
    return render_template('history_total.html')


@app.route('/history_total_data', methods=['GET', 'POST'])
def history_data():
    data = history.passenger()
    return data


@app.route('/history_inshop', methods=['GET', 'POST'])
def history_query():
    return render_template('history_inshop.html')


@app.route('/jump_deep_query',methods=['GET','POST'])
def jump_deep_query():
    return operation.deep_jump()
@app.route('/new_old', methods=['GET', 'POST'])
def new_old():
    return render_template('new_old.html')

@app.route('/new_old_query', methods=['GET', 'POST'])
def new_old_query():
    return operation.passenger_type()

@app.route('/active_rate', methods=['GET', 'POST'])
def active_rate():
    return operation.active_rate()


@app.route('/circle_query', methods=['GET', 'POST'])
def sdsd():
    return operation.visit_circle()

@app.route('/test_index',methods=['GET','POST'])
def test_index():
    return render_template('suggest.html')

@app.route('/test1',methods=['GET','POST'])
def test1():
    data=history.get_suggest('hour')
    return data


@app.route('/history_inshop_data', methods=['GET', 'POST'])
def history_data1():
    data = history.inshop_time()
    return data


@app.route('/inshop_time', methods=['GET', 'POST'])
def inshop1():

    sql = "select * from data_set"
    cur = mysqlExecute(mysqlConnect(), sql)
    max = 0
    max_name = list()
    print cur._rows[0]
    try:
        for i in cur._rows:

            if int(i[2]) >= max:
                if int(i[2]) == max:
                    max_name.append(i[0])
                else:
                    max_name = [i[0],]
                max = int(i[2])
    except Exception as e:
        print e

    ONE_PAGE_OF_DATA = 10
    try:
        curPage = int(request.args.get('curPage'))
        pageType = str(request.args.get('pageType'))
        allPage = int(request.args.get('allPage'))
    except Exception:
        curPage = 1
        allPage = 1
        pageType = ''
        # 判断点击了【下一页】还是【上一页】
    if pageType == 'pageDown':
        curPage += 1
    elif pageType == 'pageUp':
        curPage -= 1
    startPos = (curPage - 1) * ONE_PAGE_OF_DATA
    endPos = startPos + ONE_PAGE_OF_DATA
    allItemCount = len(cur._rows)
    posts = cur._rows[startPos:endPos]
    if curPage == 1 and allPage == 1:  # 标记1
        allPage = allItemCount // ONE_PAGE_OF_DATA
        remainPost = allItemCount % ONE_PAGE_OF_DATA
        if remainPost > 0:
            allPage += 1
    context = {'items': posts,
               'allPage': allPage,
               'curPage': curPage,
               'allItem': allItemCount,
               'max':[max,max_name]
               }
#    print context
    return render_template('circle_time.html',data=context)


@app.route('/hotmap', methods=['GET', 'POST'])
def hot_map():
    def getTime(delay=0, advance=0):
        return str(
            time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time() - delay + advance)))

    def Trilateration(point1,point2,point3):
        # Trilateration三边测量定位算法
        def get_two_point(point1,point2):
            import math  
            def sq(x):  
                return float(x * x)               

            R = float(point1['d'])
            R= 9
            S=8
            S = float(point2['d'])   
            x = float(point1['x'])  
            y = float(point1['y'])               
            # target point for arm  
            a = float(point2['x'])  
            b = float(point2['y'])
            print "arm target:", a, b  
            d = math.sqrt(sq(math.fabs(a-x)) + sq(math.fabs(b-y)))  
            print "desitens:", d    
            if d > (R+S) or d < (math.fabs(R-S)):  
                print "This point can't be rached!"  
                #return -1  
                exit                   
            if d == 0 and R==S :  
                print "Can't rach arm point!"  
                #return -2  
                exit              
            A = (sq(R) - sq(S) + sq(d)) / (2 * d)  
            h = math.sqrt(sq(R) - sq(A))  
              
            x2 = x + A * (a-x)/d  
            y2 = y + A * (b-y)/d  
              
            #print x2, y2  
            x3 = x2 - h * ( b - y ) / d  
            y3 = y2 + h * ( a - x ) / d  
              
            x4 = x2 + h * (b - y) / d  
            y4 = y2 - h * (a - x) / d  
              
            print "arm middle point:"  
            print x3, y3  
            print x4, y4 
    config = getConfigure('probe_position')
    probe_x1 = config['probe_x1']
    probe_x2 = config['probe_x2']
    probe_x3 = config['probe_x3']
    probe_y1 = config['probe_y1']
    probe_y2 = config['probe_y2']
    probe_y3 = config['probe_y3']
    sql = "SELECT p_range FROM inshop_time WHERE time BETWEEN '{}' AND '{}' LIMIT 1".format(getTime(24 * 3600), getTime())
    cur1 = mysqlExecute(mysqlConnect(), sql)
    sql = """SELECT phone_range ,probe_id FROM hive_data.data WHERE probe_id != 1 AND time BETWEEN '{}' AND '{}' LIMIT 1
        """.format(getTime(24 * 3600), getTime())
    cur = mysqlExecute(mysqlConnect(), sql)
    for i , j in zip(cur._rows, range(1)):
        try:
            range1 = cur1._rows[j][0]
            range2 = cur._rows[j][0]
        except Exception :
            j = 1
            range1 = cur1._rows[j][0]
            range2 = cur._rows[j][0]

        point1 = {
            'x': int(probe_x1),
            'y': int(probe_y1),
            'd': int(range1)
        }
        point2 = {
            'x': int(probe_x2),
            'y': int(probe_y2),
            'd': int(i[0])
        }
        point3 = {
            'x': int(probe_x3),
            'y': int(probe_y3),
            'd': int(range2)
        }
    Trilateration(point1, point2, point3)
    data = []
    return render_template('hot_map.html',data={"deep":data})


def start():
    app.run(debug=False, host='0.0.0.0', port=4000, threaded=True)


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=80, threaded=True)
