#! encoding:UTF-8

# ####################################################################
#   Copyright (C), team BugKiller
#   File name:  DataBase
#   Author:李政      Version:1.00        Date: 2017-04-10
#
#   Description:    数据库操作相关函数，包括mysql和hive数据库的链接，sql语句的执行函数以及数据库的初始化。
#                   其他与数据库操作相关的方法均调用此模块。
#   Function List:
#         hiveConnect()
#         mysqlConnect()
#         hiveInit()
#         mysqlInit()
#         hiveExecute()
#         mysqlExecute()
#         getConfigure()
#         writeLog()
# #####################################################################
import pyhs2
import pymysql
import time
import os


def getConfigure(options):
    """ Function:       getConfigure()
        Description:    读取数据库配置文件。
        Input:          options：区别Mysql与Hive数据库。
        Return:         ERROR code 或者包含数据库信息的列表。
        Others:         无。
    """
    config = {}
    import ConfigParser
    cf = ConfigParser.ConfigParser()
    try:
        cf.read('config.conf')
    except Exception as e:
        print e
        print "配置文件读取出错"

        return -1
    secs = cf.sections()
    if options not in secs:
        return -2
    op = cf.options(options)
    for i in op:
        config[i] = 1
    for i in config.keys():
        temp = cf.get(options, i)
        config[i] = temp
    return config



def hiveConnect():

    """ Function:       hiveConnect()
        Description:    hive数据库链接函数。
        Input:          无。
        Return:         数据库连接对象。
        Others:         无。
    """
    config = getConfigure('hive')

    if config == -1:
        print "配置文件加载出错,无法连接hive数据库"
        return
    if config == -2:
        host = raw_input("请输入hive数据库主机地址：")
        port = raw_input("请输入hiveserver2监听端口：")
        user = raw_input("请输入用户名：")
        password = raw_input("请输入密码：")
        database = raw_input("请输入数据库名称，若不存在请先创建数据库：")
        config = getConfigure('hive')

        with open("config.conf","a") as fp:
            fp.write("[hive]\n")
            fp.write("host=%s\n" % host)
            fp.write("port=%s\n" % port)
            fp.write("user=%s\n" % user)
            fp.write("password=%s\n" % password)
            fp.write("database=%s\n" % database)

    try:
        db = pyhs2.connect(host=config['host'],
                           port=int(config['port']),
                           authMechanism="PLAIN",
                           user=config['user'],
                           password=config['password'],
                           database=config['database']
                           )
    except Exception as e:
        print "hive数据库链接失败，请检查hiveserver2是否开启。"
        print e
    return db


def mysqlConnect():

    """ Function:       mysqlConnect()
        Description:    mysql数据库链接函数。
        Input:          无。
        Return:         数据库连ls
        接对象。
        Others:         无。
    """
    config = getConfigure('mysql')
    if config == -1:
        print "配置文件加载出错,无法连接mysql数据库"
        return
    if config == -2:
        host = raw_input("请输入mysql数据库主机地址：")
        user = raw_input("请输入用户名：")
        password = raw_input("请输入密码：")
        database = raw_input("请输入数据库名称，若不存在请先创建数据库：")
        config = getConfigure('mysql')

        with open("config.conf", "a") as fp:
            fp.write("[mysql]\n")
            fp.write("host=%s\n" % host)
            fp.write("user=%s\n" % user)
            fp.write("password=%s\n" % password)
            fp.write("database=%s\n" % database)
    try:
        db = pymysql.connect(host=config['host'],
                             user=config['user'],
                             password=config['password'],
                             database=config['database']
                             )
    except Exception as e:
     print e
     print config

    return db


def hiveInit(db):

    """ Function:       hiveInit()
        Description:    hive数据库初始化函数，用于创建数据库与数据表。
        Input:          数据库连接对象。
        Return:         无。
        Others:         无。
    """
    sql = """
                CREATE TABLE IF NOT EXISTS data_set(
                phone_mac varchar(50),
                time TIMESTAMP ,
                circle_time VARCHAR(10)
            """
    hiveExecute(hiveConnect(), sql)


    # 创建初始数据表
    sql = '''CREATE TABLE IF NOT EXISTS hive_data.data(
                          id int,
                          probe_id varchar(30),
                          probe_mac varchar(30),
                          rate int,
                          wssid varchar(30),
                          wmac varchar(15),
                          lat DOUBLE ,
                          lon DOUBLE ,
                          addr varchar(45),
                          phone_mac varchar(30),
                          phone_rssi int,
                          phone_range DOUBLE,
                          phone_tmc varchar(10),
                          tc boolean,
                          ds boolean,
                          ts VARCHAR(50),
                          essid varchar(80)
                          )
              PARTITIONED BY (time DATE)
              row format delimited
              fields terminated by ','
              lines terminated by '\n'
          '''
    hiveExecute(db, sql)

    sql = """
                CREATE TABLE IF NOT EXISTS history_count(
                    id int ,
                    time TIMESTAMP,
                    time_8 int,
                    time_12 int,
                    time_15 int,
                    time_18 int,
                    time_21 int,
                    inshop_8 int,
                    inshop_12 int,
                    inshop_15 int,
                    inshop_18 int,
                    inshop_21 int
                  )
                  row format delimited
              fields terminated by ','
              lines terminated by '\n'
                """
    hiveExecute(hiveConnect(), sql)


def mysqlInit(db):

    """ Function:       mysqlInit()
        Description:    mysql数据库初始化函数，用于创建数据库与数据表。
        Table Accessed: hive_data.data, hive_data.inshop。
        Table Updated:  hive_data.data, hive_data.inshop。
        Input:          数据库链接对象。
        Return:         无。
        Others:         无。
    """
    sql = """
            CREATE TABLE IF NOT EXISTS data_set(
            phone_mac varchar(50),
            time TIMESTAMP ,
            circle_time VARCHAR(10))
        """
    mysqlExecute(mysqlConnect(), sql)


    sql = "CREATE DATABASE IF NOT EXISTS hive_data"  # 创建数据库
    mysqlExecute(db, sql)
    # 创建初始数据表
    sql = """
            CREATE TABLE IF NOT EXISTS history_count(
                id int PRIMARY KEY NOT NULL auto_increment,
                time TIMESTAMP,
                time_8 int default 0,
                time_12 int default 0,
                time_15 int default 0,
                time_18 int default 0,
                time_21 int default 0,
                inshop_8 int default 0,
                inshop_12 int default 0,
                inshop_15 int default 0,
                inshop_18 int default 0,
                inshop_21 int default 0
              )
            """
    mysqlExecute(mysqlConnect(), sql)

    sql = '''CREATE TABLE IF NOT EXISTS hive_data.data(
                          id int PRIMARY KEY NOT NULL auto_increment,
                          probe_id varchar(30),
                          probe_mac varchar(30),
                          rate int,
                          wssid varchar(30),
                          wmac varchar(30),
                          lat DOUBLE DEFAULT 0,
                          lon DOUBLE DEFAULT 0,
                          addr varchar(30),
                          phone_mac varchar(30),
                          phone_rssi int,
                          phone_range DOUBLE DEFAULT 0,
                          phone_tmc varchar(10),
                          tc boolean,
                          ds boolean,
                          ts VARCHAR(50),
                          essid varchar(30),
                          time TIMESTAMP)
              '''
    mysqlExecute(db, sql)

    sql = '''
                CREATE TABLE IF NOT EXISTS hive_data.inshop_time(
                  id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
                  phone_mac VARCHAR(50),
                  time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  stay_time VARCHAR (30),
                  flag  int,
                  p_range int,
                  probe_id varchar(30))
              '''
    mysqlExecute(db,sql)

    sql = """
                create table IF NOT EXISTS data_predict(
                    all_number int(12),
                    inshop_number int(10),
                    weather int(3),
                    holiday int(2),
                    time timestamp default CURRENT_TIMESTAMP,
                    hour_temperature varchar(64),
                    hour_number varchar(128),
                    hour_number_inshop varchar(128)
                );
            """
    mysqlExecute(db, sql)

    
def mysqlExecute(db, sql):

    """ Function:       mysqlExecute()
        Description:    本函数用于在mysql数据库中执行SQL语句。
        Input:          :param db: 数据库链接对象。
                        :type db: mysql connect object.

                        :param sql: SQL语句。
                        :type sql: str.
        Return:         :return cur: object you use to interact with the database.
                        :rtype cur: object.
        Others:         无。
    """
    cur = db.cursor()
    cur.execute(sql)
    db.commit()
    return cur

def hiveExecute(db, sql):
    """ Function:       mysqlExecute()
        Description:    本函数用于在hive数据库中执行SQL语句。
        Input:          :param db: 数据库链接对象。
                        :type db: hive connect object.

                        :param sql: SQL语句。
                        :type sql: str.

        Return:         :return cur: object you use to interact with the database.
                        :rtype cur: object.
        Others:         无。
    """
    cur = db.cursor()
    cur.execute(sql)
    return cur

def writeLog(logStr, status='[OK]',):
    """ Function:       writeLog()
        Description:    日志记录函数。
        Input:          status：区别正常日志、错误日志和警告日志。
        Return:         无。
        Others:         无。
    """
    baseDir = os.path.abspath('..')
    if not os.path.exists(baseDir + '/logs'):
        os.mkdir(baseDir + '/logs')
    if not os.path.isfile(baseDir + '/logs/standard_OK_log.txt'):
        with open(baseDir + '/logs/standard_OK_log.txt','w') as fp:
            fp.write('STANDARD OK LOGs')

    if not os.path.isfile(baseDir + '/logs/standard_ERROR_log.txt'):
        with open(baseDir + '/logs/standard_ERROR_log.txt','w') as fp:
            fp.write('STANDARD ERROR LOGs')

    if not os.path.isfile(baseDir + '/logs/standard_WARNING_log.txt'):
        with open(baseDir + '/logs/standard_WARNING_log.txt','w') as fp:
            fp.write('STANDARD WARNING LOGs')

    if status.__eq__('[OK]'):
        with open(baseDir + '/logs/standard_OK_log.txt', 'a') as fp:
            fp.write(status + "\t" + str(
                time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.localtime(
                        time.time()))) + "\t\t" + logStr + '\n')

    elif status.__eq__('[ERROR]'):
        with open(baseDir + '/logs/standard_OK_log.txt', 'a') as fp:
            fp.write(status + "\t" + str(
                time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.localtime(
                        time.time()))) + "\t\t" + logStr + '\n')

    elif status.__eq__('[WARNING]'):
        with open(baseDir + '/logs/standard_OK_log.txt', 'a') as fp:
            fp.write(status + "\t" + str(
                time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.localtime(
                        time.time()))) + "\t\t" + logStr + '\n')
