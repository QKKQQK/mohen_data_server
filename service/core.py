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

async def update_min_collection(handler, record):
    datetime_begin = datetime.datetime(year=record['utc_date'].year, \
                              month=record['utc_date'].month, \
                              day=record['utc_date'].day, \
                              hour=record['utc_date'].hour, \
                              minute=record['utc_date'].minute)
    datetime_end = datetime_begin + datetime.timedelta(minutes=1)
    record_bson = record
    inc_val = helpers.generate_v_val_inc_query(record_bson)

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
                }, upsert=True, \
                return_document=pymongo.ReturnDocument.AFTER)
    inc_val_dict = {}
    inc_val_dict['v1'] = record_bson['v1']
    inc_val_dict['v2'] = record_bson['v2']
    inc_val_dict['v3'] = record_bson['v3']
    await update_min_collection_norm_val(handler, after_update_data, inc_val_dict)

async def update_min_collection_norm_val(handler, new_record, inc):
    inc_params = {}
    inc_params['v1_norm'] = helpers.log10_addition_normalize(new_record['v1'] - inc['v1'], inc['v1'])
    inc_params['v2_norm'] = helpers.log10_addition_normalize(new_record['v2'] - inc['v2'], inc['v2'])
    for key in new_record['v3'].keys():
        if key in inc['v3']:
            inc_params['v3_norm.'+str(key)] = helpers.log10_addition_normalize( \
                    new_record['v3'][key] - inc['v3'][key], inc['v3'][key])
    after_update_data = await handler.settings['db'][CONFIG.MIN_COLLECTION_NAME] \
            .find_one_and_update(
                {'_id' : new_record['_id']},
                {'$inc' : inc_params}, upsert=True)


