#!encoding:utf-8
# ####################################################################
#   File name:  perdict_info.py
#   Author:李明志      Version:1.00        Date: 2017-5-10
#
#   Description:   用于从网络中获取天气,是否是节假日和每小时的气温
#   Function List:
#         get_weather()
#         get_page()
#         get_temperature()
#         get_areaname()
# #####################################################################
import urllib
import re
import time
from selenium import webdriver
import urllib2
from bs4 import BeautifulSoup
import random
def get_weather(lat, lon, date,weathers=0):
    """ Function:       get_weather()
            Description:    从网络中获取天气状况
            Input:          经纬度和日期的判断
            Return:         :return :查询到的天气的代表值
            Others:         先用get_aeraname()将经纬度转为省市,根据查询到的结果判断天气,分为雨,雪,云,阴,晴,
                            其他六种情况,分别记为1,2,3,4,5,6
        """
    try:
        if weathers==0:
            province, city = get_areaname(lat, lon, 'EN')
            url = "http://qq.ip138.com/weather/%s/%s.htm" % (province, city)
            result = get_page(url)
            #html = urllib2.urlopen(url, timeout=20)
            #result = html.read()

            pattern = 'alt="(.+?)"'
            weather = re.findall(pattern, result)
        
            if date == 'today':
                s = weather[0]
            elif date == 'tomorrow':
                s = weather[1]
            if s.find('雨') != -1:
                return 1
            elif s.find('雪') != -1:
                return 2
            elif s.find('云') != -1:
                return 3
            elif s.find('阴') != -1:
                return 4
            elif s.find('晴') != -1:
                return 5
            return 6
        else :
            s=weathers
        if s.find(u'雨') != -1:
            return 1
        elif s.find(u'雪') != -1:
            return 2
        elif s.find(u'云') != -1:
            return 3
        elif s.find(u'阴') != -1:
            return 4
        elif s.find(u'晴') != -1:
            return 5
        return 6
    except Exception as e:
        print e
	print "func get_weather"
    return 6


def get_page(url):
    """ Function:       get_page()
                Description:    抓取所需页面,返回包含页面信息的字符串
                Input:          页面的url
                Return:         :return :包含页面信息的字符串
                Others:
            """
    result = "mark"
    try:
        html = urllib2.urlopen(url, timeout=20)
        result = html.read()
        result = unicode(result, "gbk").encode("utf8")
        return result
    except Exception as e:
        print e
	print "get_page"
    return result

def get_weektemp(lat,lon):
        """ Function:       get_weektemp()
                    Description:    根据经纬度获取地理位置
                    Input:          经纬度和日期
                    Return:         :return :地理位置
                    Others:         无
        """
        province,city=get_areaname(lat,lon,'EN')
        province=province.lower()
        city=city.lower()
        url="http://www.weaoo.com/%s/"%province
        headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
        req = urllib2.Request(url=url,headers=headers)
        context=urllib2.urlopen(req)
        context=context.read()
        bs=BeautifulSoup(context,"lxml")
        city_list=bs.findAll("ul",{"class":"city-weather-list"})
        city_list=bs.findAll("a",{"class":"city-name"})

        city_url=""
        for i in city_list:
            if i.attrs["href"].find(city)!=-1:
                city_url=i.attrs["href"]
                break
        req=urllib2.Request(url=city_url,headers=headers)
        context=urllib2.urlopen(req).read()
        bs=BeautifulSoup(context,"lxml")
        context=bs.findAll("span",{"class":"wg-wd"})
        temp=[]
        for i in context:
            s= i.string.split('~')
            high=(filter(str.isdigit,s[0].encode('gbk')))
            low=(filter(str.isdigit,s[1].encode('gbk')))
            temp.append((int(high)+int(low))/2)

        context=bs.findAll("span",{"class":"wg-tips"})
        weather=[]
        for i in context:
            s=i.string
            weather.append(get_weather("","","",s))
        return temp,weather
def get_temperature(lat, lon, flag):
    """ Function:       get_temperature()
                    Description:    获取每日8:00-22:00的气温
                    Input:          经纬度和日期
                    Return:         :return :包含气温的列表
                    Others:         无
                """
    temperature = "fail"
    try:
        province,city=get_areaname(lat,lon,'EN')
        province=province.lower()
        city=city.lower()
        url="http://www.weaoo.com/%s/"%province
        headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}  
        req = urllib2.Request(url=url,headers=headers) 
        context=urllib2.urlopen(req)
        context=context.read()
        bs=BeautifulSoup(context,"lxml")
        city_list=bs.findAll("ul",{"class":"city-weather-list"})
        city_list=bs.findAll("a",{"class":"city-name"})
        city_url=""
        for i in city_list:
            if i.attrs["href"].find(city)!=-1:
                city_url=i.attrs["href"]
                break

        req=urllib2.Request(url=city_url,headers=headers)
        context=urllib2.urlopen(req).read()
        bs=BeautifulSoup(context,"lxml")
        context=bs.findAll("span",{"class":"h-wd"})
        temp=[]
        for i in context:
            temp.append(str(i.string[:-1]))
        temperature=[]
        temperature+=temp[-11:]
        temperature+=temp[:4]       
        if flag=='tomorrow':
            for i in range(8):
                mark=random.randint(0,12)
                num=random.randint(-2,2)
                temperature[mark]=str(int(temperature[mark])+num)
            return temperature
        elif flag=='today':
            return temperature
    except Exception as e:
        print e
    return temperature

        
def get_areaname(lat, lon, lag):
    """ Function:       get_areaname()
                       Description:    根据经纬度获取省市信息
                       Input:          经纬度和查询类型
                       Return:         :return :省市信息
                       Others:         根据查询的类型来选择返回汉字还是拼音
                   """
    province = ""
    city = ""
    try:
        if(lag == 'CN'):
            url = "http://maps.google.cn/maps/api/geocode/json?latlng=%s,%s&language=CN" % (
                lat, lon)
            html = urllib.urlopen(url)
            result = html.read()
            pattern = re.compile(r'\".+省+\"')
            res = re.findall(pattern, result)
            province = res[0][15:-4]
            pattern = re.compile(r'\".+市+\"')
            res = re.findall(pattern, result)
            city = res[0][15:-4]
            return province, city
        elif(lag == 'EN'):
            url = "http://maps.google.cn/maps/api/geocode/json?latlng=%s,%s&language=EN" % (
                lat, lon)
            html = urllib.urlopen(url)
            result = html.read()
            pattern = re.compile(r'\"\w+ Sheng')
            res = re.findall(pattern, result)
            province = res[0][1:-6]
            pattern = re.compile(r'\"\w+ Shi')
            res = re.findall(pattern, result)
            city = res[0][1:-4]
            return province, city
    except Exception as e:
        print e
	print "get_areaname"
    return province, city


def get_day_type(query):
    """ Function:       get_day_type()
                           Description:    根据日期判断当天是否是节假日
                           Input:          年月日
                           Return:         :return :0或1
                           Others:         返回结果0为工作日,1为节假日
                       """
    try:
        url = 'http://www.easybots.cn/api/holiday.php?d=' + query
        req = urllib2.Request(url)
        resp = urllib2.urlopen(req)
        content = resp.read()
        if (content):
            # "0"workday, "1"leave, "2"holiday
            day_type = content[content.rfind(":") + 2:content.rfind('"')]
            if day_type == '0':
                return 0
            else:
                return 1
    except Exception as e:
        print e
    return 0