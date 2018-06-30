import tornado.ioloop
import tornado.iostream
import tornado.web
import tornado.escape
import motor.motor_tornado
import config as CONFIG
import os
import sys
import pprint
import json
import ReportRecord
import bson.json_util as bson
from bson.objectid import ObjectId
from pymongo.errors import BulkWriteError
from pprint import pprint

def validate_id(_id):
    try:
        data = json.loads(_id)
        _id_hex = data['$oid']
        return True
    except:
        return False

class UploadHandler(tornado.web.RequestHandler):
    
    async def post(self):
        data = []
        try:
            data = json.loads(self.request.body)['data']
        except Exception as e:
            self.write({'code' : 1, 'err_msg' : '\'data\'字段格式错误'})
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
                    record_bson = bson.loads(record_obj.toJSON())
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