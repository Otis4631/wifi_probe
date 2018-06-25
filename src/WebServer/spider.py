#! /usr/bin/env python
#coding:utf-8
from selenium import webdriver
import time
# ####################################################################
#   File name:  spider
#   Author:李明志      Version:1.00        Date: 2017-5-10
#   Description:   用于从网络中获取天气,是否是节假日和每小时的气温
#   Function List:
#
# #####################################################################
driver=webdriver.PhantomJS()#executable_path='../venv/local/lib/python2.7/site-packages/selenium/webdriver/phantomjs/webdriver.py')
driver.get("http://tianqi.2345.com/today-58452.htm")
time.sleep(4)
driver.close()
