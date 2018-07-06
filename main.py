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
from service import core, helpers
import docs.conf as CONFIG

class UploadHandler(tornado.web.RequestHandler):
    
    async def post(self):
        """处理 POST /data 请求

        [原始数据]：第三方上传数据
        [合并数据]：按分钟合并的原始数据

        数据库操作(最多3次，用括号中数字标注)：
        * 将[原始数据]储存进docs/conf.py定义的[原始数据]集合(1)
            * 如果_id已经存在，更新[合并数据]集合中相应的数据(2)
                * 注：覆盖已有的[原始数据]不会更改[合并数据]集合中的eid和cfg字段，
                  如果需要更改eid和cfg字段需要新建并上传一个_id不同时间相同的[原始数据]
                * 根据更新值计算归一值并更新[合并数据]集合中相应数据的归一值(3)
            * 如果_id不存在，创建或更新[合并数据】集合表中相应的数据(2)
                * 根据更新值计算归一值并更新[合并数据]集合中相应数据的归一值(3)
    
        返回：
            code:
                0：无异常
                1：请求body不存在'data' key
                2：服务器内部错误
            data:
                inserted_ids：成功创建的[原始数据]_id
                updated_ids：覆盖已有数据的[原始数据]_id
                err_ids_with_msgs：有异常的[原始数据]_id及异常信息
            count:
                success：创建/覆盖成功的[原始数据]数量
                fail：创建/覆盖失败的[原始数据]数量
                n_insert：成功创建的[原始数据]数量
                n_overwrite：成功覆盖已有数据的[原始数据]数量

        """
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
                            await core.update_min_collection(self, record_bson)
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

def main():
    db = motor.motor_tornado.MotorClient(CONFIG.DB_HOST, CONFIG.DB_PORT)[CONFIG.DB_NAME]
    application = tornado.web.Application([
        (r'/data', UploadHandler)
    ], db=db)
    application.listen(CONFIG.PORT)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()