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
from Record import RawRecord
from service import core, helpers
import docs.conf as CONFIG

class UploadHandler(tornado.web.RequestHandler):
    
    async def post(self):
        """处理 POST /data 请求

        [原始数据]：第三方上传数据
        [合并数据]：按分钟合并的原始数据

        数据库操作(每条数据3次)：
            1. 将[原始数据]储存进docs/conf.py定义的[原始数据]集合
            2. 如果_id已经存在，更新[合并数据]集合中相应的数据，
               如果_id不存在，创建或更新[合并数据】集合表中相应的数据
            3. 根据更新值计算归一值并更新[合并数据]集合中相应数据的归一值

        返回：
            code:
                0：无异常
                1：请求body不存在'data'键
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
        # 检测请求boby部分否存在'data'键
        try:
            req_data = json.loads(self.request.body)['data']
        except Exception as e:
            # 将错误信息写入输出缓冲区
            self.write({'code' : 1, 'err_msg' : '数据格式错误'})
            # 将输出缓冲区的信息输出到socket
            self.flush()
            # 结束HTTP请求
            self.finish()

        # 如果'data'键值对存在
        if req_data:
            res_inserted_ids = []  # 成功创建的数据_id
            res_updated_ids = []  # 成功覆盖原有数据的数据_id
            res_err_ids_with_msgs = []  # 触发异常的数据的_id与异常信息
            res_n_insert = 0  # 成功创建的数据数量
            res_n_overwrite = 0  # 成功覆盖原有数据的数据数量
            # 逐条处理上传的数据
            for record in req_data:
                valid_record = True  # 标注当前数据格式是否正确
                try:
                    # 使用[原始数据]创建RawRecord类，检测必需字段格式，为非必需字段插入默认值
                    record_obj = RawRecord(record)
                except Exception as e:
                    # 记录格式出错的数据的_id和异常信息
                    res_err_ids_with_msgs.append({'_id' : record['_id'] if '_id' in record else 'N/A', 'err_msg' : '数据格式错误'})
                    valid_record = False   
                try:
                    if valid_record:
                        # 将Record类转换成含有BSON数据类型的dict(例：将'$oid'转换成ObjectId类型)
                        record_bson = record_obj.to_bson()
                        # 第一次数据库操作
                        # 异步插入数据，如_id已存在，覆盖原有数据并返回原有数据，否则插入的数据并返回None
                        record_before_update = await self.settings['db'][CONFIG.RAW_COLLECTION_NAME] \
                                .find_one_and_update( \
                                    {'_id' : record_bson['_id']}, \
                                    {'$set' : record_bson}, upsert = True)
                        # 根据数据是否被覆盖记录统计数据
                        if record_before_update:
                            res_updated_ids.append(record['_id'])
                            res_n_overwrite += 1
                            # 第二，三次数据库操作
                            # 异步更新[合并数据]集合，调整更新造成的查值
                            await core.update_min_collection(self, record_bson, record_bson_old=record_before_update)
                        else:
                            res_inserted_ids.append(record['_id'])
                            res_n_insert += 1
                            # 第二，三次数据库操作
                            # 异步更新[合并数据]集合
                            await core.update_min_collection(self, record_bson)
                except Exception as e:
                    if '_id' in record:
                        res_err_ids_with_msgs.append({'_id' : record['_id'], 'err_msg' : '服务器内部错误'})
            res_code = 0 if len(res_err_ids_with_msgs) == 0 else 2
            # HTTP响应内容
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
            # 将错误信息写入输出缓冲区
            self.write(res)
            # 将输出缓冲区的信息输出到socket
            self.flush()
            # 结束HTTP请求
            self.finish()

def main():
    """配置服务端，启用事件循环

    创建Tornado实例，配置Motor异步MongoDB连接库，HTTP请求路由，
    设置端口，启用事件循环

    """
    # 配置Motor异步MongoDB连接库
    db = motor.motor_tornado.MotorClient(CONFIG.DB_HOST, CONFIG.DB_PORT)[CONFIG.DB_NAME]
    application = tornado.web.Application([
        (r'/data', UploadHandler)
    ], db=db)
    application.listen(CONFIG.PORT)
    # 启用非阻塞事件循环
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()