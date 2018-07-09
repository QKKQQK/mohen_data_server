# python 3.6 模块
import os
import string
import ast
import datetime
import sys
import json

# pip 安装模块
import bson
import pymongo
import numpy

# 本地文件，模块
import docs.conf as CONFIG
from service import helpers

async def update_min_collection(handler, record_bson):
    """更新[合并数据]的v1, v2, v3数值
    
    将相似的[原始数据]合并至同一分钟级，

    根据更新后的[合并数据]和更新后与更行前的差值生成符合$inc格式的dict，
    并更新[合并数据]的归一值

    参数：
        handler (tornado.web.RequestHandler)：Tornado的HTTP Request Handler
        record_bson (dict)：需要合并的[原始数据]，数据为BSON类型

    """
    # 计算合并时间范围下限
    datetime_begin = datetime.datetime(year=record_bson['utc_date'].year, \
                              month=record_bson['utc_date'].month, \
                              day=record_bson['utc_date'].day, \
                              hour=record_bson['utc_date'].hour, \
                              minute=record_bson['utc_date'].minute)
    # 计算合并时间范围上限
    datetime_end = datetime_begin + datetime.timedelta(minutes=1)
    # 符合$inc所需格式的v1, v2, v3增值
    inc_val = helpers.generate_v_val_inc_query(record_bson)
    # 根据条件寻找符合条件的[合并数据]并更新，可以根据情况增减条件(会影响[合并数据]集合大小)
    after_update_data = await handler.settings['db'][CONFIG.MIN_COLLECTION_NAME] \
            .find_one_and_update( \
                {'pid' : record_bson['pid'], \
                'name' : record_bson['name'], \
                'flag' : 1, \
                'exttype' : record_bson['exttype'], \
                'type' : record_bson['exttype'], \
                'tag' : record_bson['tag'], \
                'klist' : record_bson['klist'], \
                'rlist' : record_bson['rlist'], \
                'extlist' : record_bson['extlist'], \
                'ugroup' : record_bson['ugroup'], \
                'uid' : record_bson['uid'], \
                'fid' : record_bson['fid'], \
                'openid' : record_bson['openid'], \
                'utc_date' : {'$gte' : datetime_begin, \
                              '$lt' : datetime_end}
                }, \
                {'$set' : {'eid' : record_bson['eid'], \
                           'cfg' : record_bson['cfg'], \
                           'utc_date' : datetime_begin
                }, \
                '$inc' : inc_val \
                # 找不到符合条件的[合并数据]时，将创建新数据
                }, upsert=True, \
                # 返回更新后的新值
                return_document=pymongo.ReturnDocument.AFTER)
    #  dict格式的v1, v2, v3增值
    inc = {}
    inc['v1'] = record_bson['v1']
    inc['v2'] = record_bson['v2']
    inc['v3'] = record_bson['v3']
    await update_min_collection_norm_val(handler, after_update_data, inc)

async def update_min_collection_norm_val(handler, new_record, inc):
    """更新[合并数据]的归一值
    
    根据更新后的[合并数据]和更新后与更行前的差值生成符合$inc格式的dict，
    如果[合并数据]更新前归一值为0则计算归一值，否则计算归一值增值，
    并更新[合并数据]的归一值

    参数：
        handler (tornado.web.RequestHandler)：Tornado的HTTP Request Handler
        new_record (dict)：[合并数据]更新后的数据，用于计算归一值
        inc (dict)：[合并数据]数据更新后与更行前的差值，用于计算归一值

    """
    inc_params = {}
    # 如更新前归一值为0，计算v1, v2归一值，否则计算v1，v2归一值增值
    inc_params['v1_norm'] = helpers.log10_addition_normalize(new_record['v1'] - inc['v1'], inc['v1'])
    inc_params['v2_norm'] = helpers.log10_addition_normalize(new_record['v2'] - inc['v2'], inc['v2'])
    for key in new_record['v3'].keys():
        if key in inc['v3']:
            # 修改v3归一值/归一值增值格式
            inc_params['v3_norm.'+str(key)] = helpers.log10_addition_normalize( \
                    new_record['v3'][key] - inc['v3'][key], inc['v3'][key])
    # 更新归一值
    after_update_data = await handler.settings['db'][CONFIG.MIN_COLLECTION_NAME] \
            .find_one_and_update(
                {'_id' : new_record['_id']},
                {'$inc' : inc_params}, upsert=True)


