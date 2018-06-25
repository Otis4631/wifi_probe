#! encoding:UTF-8
# ####################################################################
#   File name:  operation
#   Author:李政      Version:2.31        Date: 2017-6-10
#
#   Description:   用于每日数据接收完毕后对数据的离线处理,计算来访周期和预测数据的产生
#   Function List:
#      startHDFS()
#      startWebServer()
#      startBrower()
#      isConnect()
#      startDataGatheringServer()
#      testMode()
#      WorkMode()
#      get_parser()
#      startSendData()
#
#（注:此脚本需主机支持gnome桌面环境，并且此脚本需在编译连接生成可执行文件后方可调用，否则出错）
# #####################################################################
import sys
import os
sys.path.append(os.path.abspath("."))


import webbrowser
import argparse

import time
import multiprocessing

import requests
from DataBase import DataBase


global ERROR
ERROR = False


def startHDFS():
    """ Function:       startHDFS()
        Description:    启动分布式平台
        Input:          mac地址
        Return:         :return :无
        Others:         无
    """
    cmd1 = 'gnome-terminal -x bash -c "/root/hadoop/sbin/start-all.sh"'
    cmd2 = 'gnome-terminal -x bash -c "hiveserver2;"'
    print 'hadoop 平台正在启动...'
    print 'hiveserver2 正在启动...'
    time.sleep(300)
    os.system(cmd1)
    os.system(cmd2)


def startGatherServer():
    """ Function:       startHDFS()
        Description:    启动分布式平台
        Input:          mac地址
        Return:         :return :无
        Others:         无
    """
    print '准备开启数据采集模块'
    try:
        cmd = 'gnome-terminal -x bash -c "{}/DataGatheringServer;exec bash;"'.format(
            os.path.abspath('.'))

        os.system(cmd)
    except Exception:
        try:
            from DataGathering import DataGatheringServer
            DataGatheringServer.start()
        except Exception as error:
            DataBase.writeLog(
                "采集模块开启失败，请检查服务器地址是否被占用:" + error,
                status='[ERROR]')
            print "采集模块开启失败，请检查服务器地址是否被占用:" + error
            global ERROR
            ERROR = True
            return
    DataBase.writeLog('数据采集模块开启成功')


def startSendData(host):
    """ Function:       startSendData()
        Description:    开始发送模拟数据
        Input:          主机地址
        Return:         :return :无
        Others:         无
    """
    time.sleep(10)
    print '开始发送模拟数据...'
    import sendData
    sendData.send(occurs=10, host=host)

    if not sendData.FLAG:
        global ERROR
        ERROR = True
        print '模拟数据发送出错，可能数据采集模块未开启或者地址设置有误'
        DataBase.writeLog('模拟数据发送出错，可能数据采集模块未开启或者地址设置有误', '[ERROR]')


def startWebServer():
    """ Function:       startWebServer()
        Description:    启动数据展示模块
        Input:          无
        Return:         :return :无
        Others:         无
    """
    print '准备开启数据展示模块'
    try:
        cmd = 'gnome-terminal -x bash -c "{}/web_server;exec bash;"'.format(
            os.path.abspath('.'))
        os.system(cmd)
    except Exception as error:
        try:
            import web_server
            web_server.start()

        except Exception:

            DataBase.writeLog(
                "展示模块开启失败，请检查服务器地址是否被占用:" + error,
                status='[ERROR]')
            print "展示模块开启失败，请检查服务器地址是否被占用:" + error
            global ERROR
            ERROR = True
            return
    if not ERROR:
        DataBase.writeLog('数据展示模块开启成功')


def startBrowser(host='127.0.0.1'):
    """ Function:       startBrowser()
        Description:    自动启动浏览器。
        Input:          数据报表网址
        Return:         :return :无
        Others:         无
    """
    time.sleep(10)
    i = 0
    print '正在尝试开启浏览器'
    while True:
        time.sleep(4)
        i = i + 1
        if isConnect():
            webbrowser.open('http://' + host + ':4000/chart')
            print '浏览器成功开启，正在跳转到动态图表页面'
            return
        else:
            print '通信失败'
        if i > 4:
            print '浏览器开启失败，请检查web_server是否运行在5000端口'
            DataBase.writeLog(
                '浏览器开启失败，请检查web_server是否运行在5000端口',
                status='[ERROR]')

            global ERROR
            ERROR = True
            sys.exit(2)


def isConnect(mode='testmode'):
    """ Function:       isConnect()
        Description:    各模块连通性检查函数
        Input:          启动模式
        Return:         连接成功代码或者错误代码
        Others:         无
    """
    if mode.__eq__('workmode'):
        try:
            s0 = requests.get('http://192.168.100.168', timeout=4)
            if not s0.status_code == 200:

                return False
            else:
                return True
        except Exception:
            return False
    time.sleep(3)
    try:
        s1 = requests.get('http://127.0.0.1:5000', timeout=5)
        s2 = requests.get('http://127.0.0.1:4000', timeout=5)
        if s1.status_code == s2.status_code == 200:
            flag = True
        else:
            flag = False
    except Exception:
        print '通信错误'

        flag = False
    return flag


def testMode(host):
    """ Function:       testMode()
        Description:    测试模式，以测试模式顺序启动各个模块
        Input:          主机地址
        Return:         无
        Others:         无
    """
    DataBase.mysqlInit(DataBase.mysqlConnect())
    p = multiprocessing.Pool(5)
    p.apply_async(startGatherServer())
    p.apply_async(startSendData, args=(host,))
    p.apply_async(startWebServer)
    p.apply_async(startBrowser(host))
    p.apply_async(startHDFS())
    p.close()
    p.join()
    time.sleep(2)
    print 'All subprocess started!'
    time.sleep(2)

    if not ERROR:
        DataBase.writeLog('测试模式正常开启')
        print '测试模式正常开启'
    else:
        print '测试模式开启失败'


def workMode(host):
    """ Function:       workMode()
        Description:    正常模式，以正常模式顺序启动各个模块
        Input:          主机地址
        Return:         无
        Others:         无
    """
    hive_init_error = False
    DataBase.mysqlInit(DataBase.mysqlConnect())
    try:
        DataBase.hiveInit(DataBase.hiveConnect())
    except Exception as e:
        hive_init_error = True
        print e

    if not isConnect('workmode'):
        print '探针连接失败或者探针地址设置有误，请检查探针地址与主机地址是否在同一网段'
        DataBase.writeLog('探针连接失败或者探针地址设置有误，请检查探针地址与主机地址是否在同一网段', '[ERROR]')
        sys.exit(1)

    p = multiprocessing.Pool(5)
    p.apply_async(startHDFS())
    p.apply_async(startGatherServer())
    p.apply_async(startWebServer)
    p.apply_async(startBrowser(host))

    os.system('cp hive /wifi/')
    with open("/etc/crontab", "a+") as fp:
        str = """30\t23\t*\t*\t*\troot\t/WIFI/hive"""
        fp.write(str)
    p.close()
    p.join()

    time.sleep(2)
    if hive_init_error:
        try:
            DataBase.hiveInit(DataBase.hiveConnect())
        except Exception:
            DataBase.writeLog("hive数据表初始化失败！", '[ERROR]')

    print 'All subprocess started!'
    time.sleep(2)

    if not ERROR:
        DataBase.writeLog('工作模式正常开启')

    DataBase.writeLog('工作模式正常开启')
    print '工作模式正常开启'


def get_parser():
    """ Function:       get_parser()
        Description:    生成命令行参数以及帮助信息
        Input:          无
        Return:         无
        Others:         无
    """
    parser = argparse.ArgumentParser(
        description='setup script to init test.py mode or work mode')
    parser.add_argument(
        '-m',
        '--mode',
        help='run mode, choose a mode of test.py or work (default: testmode)',
        type=str,
    )
    parser.add_argument(
        '-a',
        '--address',
        help='host address of database or web server',
        type=str,
        default='127.0.0.1')

    return parser


def main():
    #主程序
    path = os.path.abspath('.')
    if not os.path.isfile(path + '/start'):
        print "请在脚本目录下运行本脚本！"
        # sys.exit(0)
    parser = get_parser()
    args = vars(parser.parse_args())
    DataBase.mysqlInit(DataBase.mysqlConnect())
    if not args['mode']:
        parser.print_help()
        return
    mode = args['mode']
    print args.items()

    if mode.__eq__('test'):
        testMode(args['address'])

        return
    if mode.__eq__('work'):
        workMode(args['address'])
        return
    else:
        print 'only work mode and test mode can be chosen'


if __name__ == '__main__':
    if not os.path.isfile('config.conf'):
        with open("config.conf", 'w') as fp:
            fp.write("#This is configure files\n")
    main()
