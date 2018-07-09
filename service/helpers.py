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

def log10_normalize(n):
    """使用log10公式计算归一值

    使用log10(n)/log10(最大值)计算n的归一值，最大值在CONFIG.py中定义

    参数：
        n (int/float)：需要计算归一值的数值

    返回：
        float：使用log10公式计算后的归一值

    """
    if n <= 1:
        return 0.0
    norm = numpy.log10(n) / numpy.log10(CONFIG.LOG10_MAX)
    return norm


def log10_addition_normalize(a, b):
    """计算的log10(a + b)/log10(最大值)与log10(a)/log10(最大值)的差值
    
    使用log10(1 + b/a)/log10(最大值)计算差值，最大值在CONFIG.py中定义
    基于公式：log10(a + b) = log(a) + log(1 + b/a)
    如果a值为0(即第一次插入数据的情况)，使用log10_normalize(b)计算

    参数：
        a (int/float)：数据更新前的数值，a >= 0
        b (int/float)：数据更新后与更行前的差值，b >= 0

    返回：
        float：计算后的归一值差值

    """
    if a == 0:
        return log10_normalize(b)
    return numpy.log10(1 + b/a) / numpy.log10(CONFIG.LOG10_MAX)