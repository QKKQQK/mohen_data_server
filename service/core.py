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
from service import helpers
from docs import conf as CONFIG

def get_time_range(record_bson):
    begin = datetime.datetime(year=record_bson['utc_date'].year, \
                              month=record_bson['utc_date'].month, \
                              day=record_bson['utc_date'].day, \
                              hour=record_bson['utc_date'].hour, \
                              minute=record_bson['utc_date'].minute)
    # 计算合并时间范围上限
    end = begin + datetime.timedelta(minutes=1)
    return begin, end

def should_combine(record_bson, record_bson_old):
    combine = True
    try:
        attr_list = ['pid', 'name', 'exttype', 'type', 'tag', 'klist', 'rlist', 'extlist', 'ugroup', 'uid', 'fid', 'openid']
        for attr in attr_list:
            combine = combine and (record_bson[attr] == record_bson_old[attr])
        record_bson_begin, record_bson_end = get_time_range(record_bson)
        record_bson_old_begin, record_bson_old_end = get_time_range(record_bson_old)
        combine = combine and (record_bson_begin == record_bson_old_begin) and (record_bson_end == record_bson_old_end)
    except Exception as e:
        print(e)
        sys.stdout.flush()
        combine = False
    return combine

def generate_v_val_inc_query(record_bson, record_bson_old={}, sign=1):
    """生成符合find_one_and_update中$inc要求格式的v1，v2，v3值

    将dict格式的v3值从v3 : {attr : val}转化为v3.attr : val
    在结果dict中插入v1，v2，修改格式后的v3并返回

    参数：
        record_bson (dict)：需要合并的[原始数据]，数据为BSON类型
        record_bson_old (dict)：更新[原始数据]的情况下[原始数据]的原值，数据为BSON类型
        sign (int)：1或-1，用于调整inc的正负，当需要减去record_bson的时候设为-1

    返回：
        dict：修改格式后的v1，v2，v3值

    """
    result = {}
    result_dict = {}
    result['v1'] = sign * (record_bson['v1'] - (record_bson_old['v1'] if record_bson_old else 0))
    result['v2'] = sign * (record_bson['v2'] - (record_bson_old['v2'] if record_bson_old else 0))
    # # 当新值v3为空时，如果这是第一次插入record或者原有record的v3为空时，删除v3原始值{}
    # if not record_bson['v3'] and ((not record_bson_old) or (not record_bson_old['v3'])):
    #     del result['v3']
    result_dict['v1'] = result['v1']
    result_dict['v2'] = result['v2']
    result_dict['v3'] = {}
    for key in record_bson['v3'].keys():
        result['v3.'+key] = sign * (record_bson['v3'][key] - (record_bson_old['v3'][key] \
                                    if record_bson_old and key in record_bson_old['v3'] else 0))
        result_dict['v3'][key] = result['v3.'+key]
    
    if record_bson_old:
        for key in record_bson_old['v3'].keys():
            if key not in record_bson['v3'].keys():
                result['v3.'+key] = -record_bson_old['v3'][key]
                result_dict['v3'][key] = result['v3.'+key]
    print(result)
    sys.stdout.flush()
    return result, result_dict

async def update_combined_collection(handler, record_bson, record_bson_old={}):
    """更新[合并数据]的v1, v2, v3数值，将相似的[原始数据]合并至同一分钟级
    
    根据[原始数据]原值与新值生成符合$inc格式的v1，v2，v3增值dict
    *注：当原值存在新值没有的键时，更新[合并数据]时，减去该键值和键值的归一值
    更新[合并数据]除归一值外的值(第二次数据库操作)

    参数：
        handler (tornado.web.RequestHandler)：Tornado的HTTP Request Handler
        record_bson (dict)：需要合并的[原始数据]，数据为BSON类型
        record_bson_old (dict)：更新[原始数据]的情况下[原始数据]的原值，数据为BSON类型

    """

    # 原始数据操作为插入
    if not record_bson_old:
        inc, inc_dict = generate_v_val_inc_query(record_bson)
        await update_combined_collection_val(handler, record_bson, inc, inc_dict)
    # 原始数据操作为更新
    else:
        # 将无效数据更改至有效时，合并数据加新数据，更新归一值
        if record_bson_old['flag'] == 0 and record_bson['flag'] == 1:
            inc, inc_dict = generate_v_val_inc_query(record_bson)
            await update_combined_collection_val(handler, record_bson, inc, inc_dict)
        # 将有效数据更改至无效时，合并数据减旧数据，更新归一值
        elif record_bson_old['flag'] == 1 and record_bson['flag'] == 0:
            inc, inc_dict = generate_v_val_inc_query(record_bson_old, sign=-1)
            await update_combined_collection_val(handler, record_bson_old, inc, inc_dict)
        # 修改有效数据时，合并数据加新旧差值，更改归一值
        elif record_bson_old['flag'] == 1 and record_bson['flag'] == 1:
            # 新，旧原始数据在同一合并数据范围
            if should_combine(record_bson, record_bson_old):
                inc, inc_dict = generate_v_val_inc_query(record_bson, record_bson_old=record_bson_old)
                await update_combined_collection_val(handler, record_bson, inc, inc_dict)
            # 新，旧原始数据在不同的合并数据范围
            else:
                # 旧数据所属合并数据减旧数据，更新归一值
                inc, inc_dict = generate_v_val_inc_query(record_bson_old, sign=-1)
                await update_combined_collection_val(handler, record_bson_old, inc, inc_dict)
                # 新数据所属合并数据加新数据，更新归一值
                inc, inc_dict = generate_v_val_inc_query(record_bson)
                await update_combined_collection_val(handler, record_bson, inc, inc_dict)
        # 当新旧数据flag都为0时，无需更改合并数据
        else:
            pass

async def update_combined_collection_val(handler, record_bson, inc, inc_dict):
    # 计算合并时间范围下限
    datetime_begin, datetime_end = get_time_range(record_bson)

    after_update_data = await handler.settings['db'][CONFIG.COMBINED_COLLECTION_NAME] \
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
                           'utc_date' : datetime_begin, \
                           'version' : handler.settings['version']
                }, \
                '$inc' : inc \
                # 找不到符合条件的[合并数据]时，将创建新数据
                }, upsert=True, \
                # 返回更新后的新值
                return_document=pymongo.ReturnDocument.AFTER)
    await update_combined_collection_norm_val(handler, after_update_data, inc_dict)

async def update_combined_collection_norm_val(handler, new_record, inc):
    """更新[合并数据]的归一值
    
    根据[合并数据]原值与新值生成符合$inc格式的归一值增值dict
    更新[合并数据]的归一值(第三次数据库操作)

    参数：
        handler (tornado.web.RequestHandler)：Tornado的HTTP Request Handler
        new_record (dict)：[合并数据]更新后的数据，用于计算归一值
        inc (dict)：[合并数据]数据更新后与更行前的差值，用于计算归一值

    """
    version = handler.settings['version']
    inc_params = {}
    # 如更新前归一值为0，计算v1, v2归一值，否则计算v1，v2归一值增值
    inc_params['v1_norm'] = helpers.log10_addition_normalize(new_record['v1'] - inc['v1'], inc['v1'], version)
    inc_params['v2_norm'] = helpers.log10_addition_normalize(new_record['v2'] - inc['v2'], inc['v2'], version)
    for key in new_record['v3'].keys():
        if key in inc['v3']:
            # 修改v3归一值/归一值增值格式
            inc_params['v3_norm.'+str(key)] = helpers.log10_addition_normalize( \
                    new_record['v3'][key] - inc['v3'][key], inc['v3'][key], version)
    # 第三次数据库操作
    # 更新归一值
    after_update_data = await handler.settings['db'][CONFIG.COMBINED_COLLECTION_NAME] \
            .find_one_and_update(
                {'_id' : new_record['_id']},
                {'$inc' : inc_params}, upsert=True)

async def sample_for_version(db):
    doc = await db[CONFIG.COMBINED_COLLECTION_NAME].find_one({'flag' : 1})
    return doc['version']

async def version_check(db, input_version):
    current_version = await sample_for_version(db)
    if current_version != input_version:
        res = input("Input version v"+str(input_version)+" does not match with current version v"+str(current_version)+", proceed? [Y/N] ")
        if res == 'Y' or res == 'y':
            pass
        else:
            sys.exit()
    else:
        print("Application version v" + str(current_version))
        sys.stdout.flush()

async def update_norm_to_version(db, version):
    current_version = await sample_for_version(db)
    res = input("Update data from version v" + str(current_version) + " to version v" + str(version) + ", proceed? [Y/N] ")
    if res == 'Y' or res == 'y':
        pass
    else:
        sys.exit()
    print("Updating data to version v" + str(version) + " ...")
    sys.stdout.flush()
    cursor = db[CONFIG.COMBINED_COLLECTION_NAME].find({'version' : {'$ne' : version}})
    count = 0
    for doc in await cursor.to_list(length=CONFIG.TO_LIST_BUFFER_LENGTH):
        set_params = {}
        set_params['version'] = version
        set_params['v1_norm'] = helpers.log10_normalize(doc['v1'], version)
        set_params['v2_norm'] = helpers.log10_normalize(doc['v2'], version)
        for key in doc['v3'].keys():
            # 修改v3归一值/归一值增值格式
            set_params['v3_norm.'+str(key)] = helpers.log10_normalize( \
                    doc['v3'][key], version)
        db[CONFIG.COMBINED_COLLECTION_NAME].update_one({'_id' : doc['_id']}, \
                {'$set' : set_params})
        count += 1
    print('Update complete,', count, 'documents updated')
    print("Application version v" + str(version))
    sys.stdout.flush()

async def clean_up_empty_combined_data(db):
    print('Cleaning up empty combined data...')
    sys.stdout.flush()
    cursor = db[CONFIG.COMBINED_COLLECTION_NAME].find({'v1' : {'$eq' : 0}, 'flag' : 1})
    count = 0
    for doc in await cursor.to_list(length=CONFIG.TO_LIST_BUFFER_LENGTH):
        await db[CONFIG.COMBINED_COLLECTION_NAME].update_one({'_id': doc['_id']}, {'$set': {'flag': 0}})
        count += 1
    print('Cleanup complete,', count, 'documents deleted')
    sys.stdout.flush()
    