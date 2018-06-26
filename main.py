import tornado.ioloop
import tornado.iostream as iostream
import tornado.web
import tornado.gen as gen
import motor.motor_tornado
import config as CONFIG
import os
import pprint
from bson.json_util import dumps

class UploadHandler(tornado.web.RequestHandler):
    
    async def post(self):
        pass

def main():
    db = motor.motor_tornado.MotorClient(CONFIG.DB_HOST, CONFIG.DB_PORT)[CONFIG.DB_NAME]

    application = tornado.web.Application([
        (r'/upload', UploadHandler)
    ], db=db)

    application.listen(CONFIG.PORT)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()