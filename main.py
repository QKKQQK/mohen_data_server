import tornado.ioloop
import tornado.iostream
import tornado.web
import tornado.escape
import motor.motor_tornado
import config as CONFIG
import os
import string
import ast
import sys
import pprint
import bson.errors
import numbers
import json
import ReportRecord
import bson.json_util
from bson.objectid import ObjectId
from pymongo.errors import BulkWriteError
from pymongo import ReturnDocument
from pprint import pprint
from datetime import datetime, timedelta
import numpy

def log10_normalize(input):
    if input <= 1:
        return 0.0
    norm = numpy.log10(input) / numpy.log10(CONFIG.LOG10_MAX)
    return norm

def log10_add_normalize(a, b):
    if a == 0:
        return log10_normalize(b)
    return numpy.log10(1 + b/a) / numpy.log10(CONFIG.LOG10_MAX)

def generate_v_val_inc_query(record_bson):
    result = {}
    result['v1'] = record_bson['v1']
    result['v2'] = record_bson['v2']
    for key in record_bson['v3'].keys():
        result['v3.'+str(key)] = record_bson['v3'][key]
    return result

class UploadHandler(tornado.web.RequestHandler):
    
    async def post(self):
        data = []
        try:
            data = json.loads(self.request.body)['data']
        except Exception as e:
            self.write({'code' : 1, 'err_msg' : '数据格式错误'})
            self.flush()
            self.finish()

        if data:
            inserted_ids = []
            updated_ids = []
            err_ids_with_msgs = []
            n_insert = 0
            n_overwrite = 0
            for record in data:
                try:
                    record_obj = ReportRecord.ReportRecord(record)
                except Exception as e:
                    err_ids_with_msgs.append({'_id' : record['_id'] if '_id' in record else 'N/A', 'err_msg' : '数据格式错误'})   
                try:
                    if '_id' in record:
                        record_bson = bson.json_util.loads(record_obj.toJSON())
                        existing_data = await self.settings['db'][CONFIG.RAW_COLLECTION_NAME] \
                                .find_one_and_update( \
                                    {'_id' : record_bson['_id']}, \
                                    {'$set' : record_bson}, upsert = True)
                        if existing_data:
                            updated_ids.append(record['_id'])
                            n_overwrite += 1
                        else:
                            inserted_ids.append(record['_id'])
                            n_insert += 1
                            await self.update_min_collection(record_bson)
                        
                except Exception as e:
                    if '_id' in record:
                        err_ids_with_msgs.append({'_id' : record['_id'] if '_id' in record else 'N/A', 'err_msg' : str(e)})
            code = 0 if len(err_ids_with_msgs) == 0 else 2

            res = {
                'code' : code,
                'data' : {
                    'inserted_ids' : inserted_ids,
                    'updated_ids' : updated_ids,
                    'err_ids_with_msgs' : err_ids_with_msgs
                },
                'count' : {
                    'success' : len(inserted_ids) + len(updated_ids),
                    'fail' : len(err_ids_with_msgs),
                    'n_insert' : n_insert,
                    'n_overwrite' : n_overwrite
                }
            }
            self.write(res)
            self.flush()
            self.finish()

    async def update_min_collection(self, record):
        datetime_begin = datetime(year=record['utc_date'].year, \
                                  month=record['utc_date'].month, \
                                  day=record['utc_date'].day, \
                                  hour=record['utc_date'].hour, \
                                  minute=record['utc_date'].minute)

        datetime_end = datetime_begin + timedelta(minutes=1)
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
                    return_document=ReturnDocument.AFTER)
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
        inc_params['v1_norm'] = log10_add_normalize(new_record['v1'] - inc['v1'], inc['v1'])
        inc_params['v2_norm'] = log10_add_normalize(new_record['v2'] - inc['v2'], inc['v2'])
        for key in new_record['v3'].keys():
            if key in inc['v3']:
                inc_params['v3_norm.'+str(key)] = log10_add_normalize( \
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