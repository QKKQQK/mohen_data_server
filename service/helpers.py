# python 3.6 模块
import os
import string
import ast
import datetime
import sys
import json

# pip 安装模块
import bson
import numpy

# 本地文件，模块
import docs.conf as CONFIG

def generate_v_val_inc_query(record_bson):
    """生成符合find_one_and_update中$inc要求格式的v1，v2，v3值

    将dict格式的v3值从v3 : {attr : val}转化为v3.attr : val
    在结果dict中插入v1，v2，修改格式后的v3并返回

    参数：
        record_bson (dict)：单条数据，id类数值都为bson ObjectId格式

    返回：
        dict：修改格式后的v1，v2，v3值

    """
    result = {}
    result['v1'] = record_bson['v1']
    result['v2'] = record_bson['v2']
    for key in record_bson['v3'].keys():
        result[''.join(['v3.', key])] = record_bson['v3'][key]
    return result