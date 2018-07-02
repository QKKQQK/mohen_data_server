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
import json
import ReportRecord
import bson.json_util
from bson.objectid import ObjectId
from pymongo.errors import BulkWriteError
from pprint import pprint

def log10_normalize(input):
    if input <= 1:
        return 0.0
    norm = numpy.log10(input) / numpy.log10(CONFIG.LOG10_MAX)
    return norm if norm < 1.0 else 1.0

def validate_id_input_type(record):
    try:
        if not isinstance(record['_id'], dict):

    except Exception:
        return False

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
                except ValueError as e:
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

def main():
    db = motor.motor_tornado.MotorClient(CONFIG.DB_HOST, CONFIG.DB_PORT)[CONFIG.DB_NAME]

    application = tornado.web.Application([
        (r'/upload', UploadHandler)
    ], db=db)

    application.listen(CONFIG.PORT)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()