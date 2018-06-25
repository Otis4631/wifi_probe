#! encoding:UTF-8
# ####################################################################
#   Copyright (C), team BugKiller
#   File name:  DataImport
#   Author:李明志      Version:2.10        Date: 2017-04-14
#
#   Description:读取配置WIFI探针距离与位置的文件。    
#   Function List:
#         read_xml()
#         set_data()
#         get_data()
# #####################################################################

from xml.etree.ElementTree import ElementTree, Element
import os
def read_xml(in_path):
    '''读取并解析xml文件
      in_path: xml路径
      return: ElementTree'''
    tree = ElementTree()
    tree.parse(in_path)
    return tree

    
def set_data(node,data):
    path = os.path.abspath('.') + '/info.xml'
    tree=read_xml(path)
    print "info.xml:" + path
    nodelist=tree.findall(node)
    for node in nodelist:
        node.text=data
    tree.write('info.xml', encoding="utf-8", xml_declaration=True)


def get_data(node):
    tree=read_xml('info.xml')
    nodelist=tree.findall(node)
    for node in nodelist:
        return node.text
