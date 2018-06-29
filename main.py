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
from bson.json_util import dumps
from bson.json_util import loads
from pymongo.errors import BulkWriteError
from pprint import pprint

class UploadHandler(tornado.web.RequestHandler):
    
    async def post(self):
        data = []
        try:
            data = loads(self.request.body)['data']
        except Exception as e:
            self.write({"code" : 1, "err_msg" : str(e)})
            self.flush()
            self.finish()

        if data: 
            success_id = []
            err_with_id = []
            n_insert = 0
            n_overwrite = 0
            for record in data:
                try:
                    existing_data = await self.settings['db'][CONFIG.RAW_COLLECTION_NAME].find_one_and_update( \
                        {'_id' : record['_id']}, {'$set' : record}, upsert = True, return_document=False)
                    success_id.append(json.loads(dumps(record['_id'])))
                    if existing_data:
                        n_overwrite += 1
                    else:
                        n_insert += 1
                except Exception as e:
                    err_with_id.append({'_id' : json.loads(dumps(record['_id'])), 'err_msg' : str(e)})
            code = 0 if len(err_with_id) == 0 else 1
            res = {
                'code' : code,
                'data' : {
                    'success_id' : success_id,
                    'err_with_id' : err_with_id,
                },
                'count' : {
                    'success' : len(success_id),
                    'fail' : len(err_with_id),
                    'n_insert' : n_insert,
                    'n_overwrite' : n_overwrite
                }
            }
            self.write(res)
            self.flush()
            self.finish()

        # except BulkWriteError as err:
        #     res = {
        #         "code" : 1,
        #         "mongo_code" : err.details["writeErrors"][0]["code"],
        #         "err_record" : err.details["writeErrors"][0]["op"],
        #         "err_index" : err.details["writeErrors"][0]["index"],
        #         "n_inserted" : err.details["nInserted"]
        #     }
        #     self.write(dumps(res))
        #     self.flush()
        # result = await self.settings['db'][CONFIG.RAW_COLLECTION_NAME].insert_many(data)
            

def main():
    db = motor.motor_tornado.MotorClient(CONFIG.DB_HOST, CONFIG.DB_PORT)[CONFIG.DB_NAME]

    application = tornado.web.Application([
        (r'/upload', UploadHandler)
    ], db=db)

    application.listen(CONFIG.PORT)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()