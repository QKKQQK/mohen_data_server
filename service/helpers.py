# python 3.6 模块
import uuid

# pip 安装模块
import numpy

# 本地文件，模块
import docs.conf as CONFIG

def get_UUID():
    return str(uuid.uuid1())

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
    """计算归一值差值，适用于将新的[原始数据]合并至已有[合并数据]或创建新的[合并数据]的情况
    
    基于公式：log(a + b) = log(a) + log(1 + b/a)
    使用log10(1 + b/a)/log10(最大值)计算差值，最大值在CONFIG.py中定义
    如果a值为0(即第一次插入数据的情况)，使用log10_normalize(b)计算

    参数：
        a (int/float)：数据更新前的数值，a >= 0
        b (int/float)：数据更新后与更行前的差值，b >= 0

    返回：
        float：计算后的归一值差值

    """
    if a == 0:
        return log10_normalize(b)
    if a <= -b:
        return -log10_normalize(a)
    return numpy.log10(1 + b/a) / numpy.log10(CONFIG.LOG10_MAX)

print(get_UUID())