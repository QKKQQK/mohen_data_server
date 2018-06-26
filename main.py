import tornado.ioloop
import tornado.iostream
import tornado.web
import tornado.escape
import motor.motor_tornado
import config as CONFIG
import os
import sys
import pprint
from bson.json_util import dumps
from bson.json_util import loads
from pymongo.errors import BulkWriteError
from pprint import pprint

class UploadHandler(tornado.web.RequestHandler):
    
    async def post(self):
        try: 
            data = loads(self.request.body)
            result = await self.settings['db'][CONFIG.RAW_COLLECTION_NAME].insert_many(data['data'])
            res = {
                "code" : 0,
                "inserted_ids" : result.inserted_ids
            }
            self.write(dumps(res))
            self.flush()
        except BulkWriteError as err:
            self.write(dumps(err.details))
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