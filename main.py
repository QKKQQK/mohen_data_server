# python 3.6 模块
import os
import string
import ast
import datetime
import sys
import json

# pip 安装模块
import tornado.ioloop
import tornado.iostream
import tornado.web
import tornado.escape
import motor.motor_tornado
import bson
import pymongo
import numpy

# 本地文件，模块
from ReportRecord import ReportRecord as Record
import CONFIG


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
    norm = numpy.log10(input) / numpy.log10(CONFIG.LOG10_MAX)
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

def generate_v_val_inc_query(record_bson):
    """生成符合find_one_and_update中$inc要求格式的v1，v2，v3值

    将dict格式的v3值从v3 : {attr : val}转化为v3.attr : val
    在结果dict中插入v1，v2，修改格式后的v3并返回

    参数：
        record_bson (dict)：单条数据，id类数值都为bson ObjectId格式

    返回：
        dict：修改格式后的v1，v2，v3值

    """
    print(record_bson)
    result = {}
    result['v1'] = record_bson['v1']
    result['v2'] = record_bson['v2']
    for key in record_bson['v3'].keys():
        result[''.join(['v3.', key])] = record_bson['v3'][key]
    return result

class UploadHandler(tornado.web.RequestHandler):
    
    async def post(self):
        req_data = []
        try:
            req_data = json.loads(self.request.body)['data']
        except Exception as e:
            self.write({'code' : 1, 'err_msg' : '数据格式错误'})
            self.flush()
            self.finish()

        if req_data:
            res_inserted_ids = []
            res_updated_ids = []
            res_err_ids_with_msgs = []
            res_n_insert = 0
            res_n_overwrite = 0
            for record in req_data:
                try:
                    record_obj = Record(record)
                except Exception as e:
                    res_err_ids_with_msgs.append({'_id' : record['_id'] if '_id' in record else 'N/A', 'err_msg' : '数据格式错误'})   
                try:
                    if '_id' in record:
                        record_bson = bson.json_util.loads(record_obj.toJSON())
                        record_before_update = await self.settings['db'][CONFIG.RAW_COLLECTION_NAME] \
                                .find_one_and_update( \
                                    {'_id' : record_bson['_id']}, \
                                    {'$set' : record_bson}, upsert = True)
                        if record_before_update:
                            res_updated_ids.append(record['_id'])
                            res_n_overwrite += 1
                        else:
                            res_inserted_ids.append(record['_id'])
                            res_n_insert += 1
                            await self.update_min_collection(record_bson)
                except Exception as e:
                    if '_id' in record:
                        res_err_ids_with_msgs.append({'_id' : record['_id'], 'err_msg' : '服务器内部错误'})
            res_code = 0 if len(res_err_ids_with_msgs) == 0 else 2

            res = {
                'code' : res_code,
                'data' : {
                    'inserted_ids' : res_inserted_ids,
                    'updated_ids' : res_updated_ids,
                    'err_ids_with_msgs' : res_err_ids_with_msgs
                },
                'count' : {
                    'success' : len(res_inserted_ids) + len(res_updated_ids),
                    'fail' : len(res_err_ids_with_msgs),
                    'n_insert' : res_n_insert,
                    'n_overwrite' : res_n_overwrite
                }
            }
            self.write(res)
            self.flush()
            self.finish()

    async def update_min_collection(self, record):
        datetime_begin = datetime.datetime(year=record['utc_date'].year, \
                                  month=record['utc_date'].month, \
                                  day=record['utc_date'].day, \
                                  hour=record['utc_date'].hour, \
                                  minute=record['utc_date'].minute)

        datetime_end = datetime_begin + datetime.timedelta(minutes=1)
        record_bson = record
        inc_val = generate_v_val_inc_query(record_bson)

        
        after_update_data = await self.settings['db'][CONFIG.MIN_COLLECTION_NAME] \
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
        await self.update_min_collection_norm_val(after_update_data, inc_val_dict)

    async def update_min_collection_norm_val(self, new_record, inc):
        print(new_record)
        print(inc)
        sys.stdout.flush()
        inc_params = {}
        inc_params['v1_norm'] = log10_addition_normalize(new_record['v1'] - inc['v1'], inc['v1'])
        inc_params['v2_norm'] = log10_addition_normalize(new_record['v2'] - inc['v2'], inc['v2'])
        for key in new_record['v3'].keys():
            if key in inc['v3']:
                inc_params['v3_norm.'+str(key)] = log10_addition_normalize( \
                        new_record['v3'][key] - inc['v3'][key], inc['v3'][key])
        print(inc_params)
        sys.stdout.flush()
        after_update_data = await self.settings['db'][CONFIG.MIN_COLLECTION_NAME] \
                .find_one_and_update(
                    {'_id' : new_record['_id']},
                    {'$inc' : inc_params}, upsert=True)

def main():
    db = motor.motor_tornado.MotorClient(CONFIG.DB_HOST, CONFIG.DB_PORT)[CONFIG.DB_NAME]
    application = tornado.web.Application([
        (r'/upload', UploadHandler)
    ], db=db)
    application.listen(CONFIG.PORT)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()