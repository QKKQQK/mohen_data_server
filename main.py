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
from pprint import pprint
from datetime import datetime, timedelta
import numpy

def log10_normalize(input):
    if input <= 1:
        return 0.0
    norm = numpy.log10(input) / numpy.log10(CONFIG.LOG10_MAX)
    return norm if norm < 1.0 else 1.0

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
                        
                except Exception as e:
                    if '_id' in record:
                        err_ids_with_msgs.append({'_id' : record['_id'] if '_id' in record else 'N/A', 'err_msg' : str(e)})
                await self.save_data_by_minute(record_bson)
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

    async def save_data_by_minute(self, record):
        v1_norm = log10_normalize(record['v1'])
        v2_norm = 0
        v3_norm = {}
        if 'v2' in record:
            v2_norm = log10_normalize(record['v2'])
        for key in record['v3'].keys():
            v3_norm[(str(key)+'_norm')] = log10_normalize(record['v3'][key])
        print('v1_norm', v1_norm)
        print('v2_norm', v2_norm)
        print('v3_norm', v3_norm)

        datetime_begin = record['utc_date']

        datetime_end = datetime_begin + timedelta(minutes=1)

        #try:
        record_bson = record
        print(type(datetime_begin))
        print(datetime_begin)
        print(type(datetime_end))
        print(datetime_end)
        sys.stdout.flush()
        existing_data = await self.settings['db'][CONFIG.MIN_COLLECTION_NAME] \
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
                    '$inc' : generate_v_val_inc_query(record_bson) \
                    }, upsert = True)
        return 1
        # except Exception as e:
        #     print(str(e))
        #     sys.stdout.flush()
        #     return 0

def main():
    db = motor.motor_tornado.MotorClient(CONFIG.DB_HOST, CONFIG.DB_PORT)[CONFIG.DB_NAME]

    application = tornado.web.Application([
        (r'/upload', UploadHandler)
    ], db=db)

    application.listen(CONFIG.PORT)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()