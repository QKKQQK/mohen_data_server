# python 3.6 模块
import os
import string
import ast
import datetime
import sys
import getopt
import json
import time
import csv

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
from Search import Search
from Search import TreeNode
from service import core, helpers
from docs import conf as CONFIG

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
                    res_err_ids_with_msgs.append({'_id' : record['_id'] if '_id' in record else 'N/A', 'err_msg' : '对象化失败，请检查数据格式'})
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
                            await core.update_combined_collection(self, record_bson, record_bson_old=record_before_update)
                        else:
                            res_inserted_ids.append(record['_id'])
                            res_n_insert += 1
                            # 第二，三次数据库操作
                            # 当flag为1时，异步更新[合并数据]集合
                            if record_bson['flag']:
                                await core.update_combined_collection(self, record_bson)
                except Exception as e:
                    if '_id' in record:
                        res_err_ids_with_msgs.append({'_id' : record['_id'], 'err_msg' : str(e)})
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

class SearchHandler(tornado.web.RequestHandler):
    async def post(self):
        req_data = []
        req_metadata = []
        # 检测请求body部分否存在'data'与'metadata'键
        try:
            req_data = json.loads(self.request.body)['data']
            req_data = bson.json_util.loads(json.dumps(req_data))
            req_metadata = json.loads(self.request.body)['metadata']
            req_metadata = bson.json_util.loads(json.dumps(req_metadata))
        except Exception as e:
            # HTTP响应内容
            res = {
                'code' : 1, 
                'err_msg' : '数据格式错误'
            }
            # 将错误信息写入输出缓冲区
            self.write(res)
            # 将输出缓冲区的信息输出到socket
            self.flush()
            # 结束HTTP请求
            self.finish()

        if req_data and req_metadata:
            if 'file' in req_metadata:
                cursor = Search(req_data).to_query(self.settings['db'])
                # 不返回文件的情况
                if not req_metadata['file']:
                    # MotorCursor，这一步不进行I/O
                    result = []
                    try: 
                        # to_list()每次缓冲length条文档，执行I/O
                        for doc in await cursor.to_list(length=CONFIG.TO_LIST_BUFFER_LENGTH):
                            result += [doc]
                        # 将BSON格式结果转换成JSON格式
                        result = json.loads(bson.json_util.dumps(result))
                        # HTTP响应内容
                        res = {
                            'code' : 0, 
                            'data' : result,
                            'count': {
                                'n_record' : len(result)
                            }
                        }
                        self.write(res)
                        self.flush()
                        self.finish()
                    except Exception:
                        res = {
                            'code' : 1, 
                            'err_msg' : '数据格式错误'
                        }
                        self.write(res)
                        self.flush()
                        self.finish()
                # 返回文件的情况
                else:
                    if 'tree' in req_metadata:
                        if req_metadata['tree']:
                            has_error = False
                            try:
                                res_uuid = helpers.get_UUID()
                                has_result = False
                                # 树形结构参数
                                tree_group_type = req_metadata["tree_group_type"]
                                tree_attr_proj = req_metadata["tree_attr_proj"]
                                tree_path_attr = req_metadata["path"]
                                show_raw_data = req_metadata["show_raw_data"]
                                # csv 文件 header
                                fieldnames = ['node_id', 'children', 'is_leaf', 'is_root']
                                for attr in tree_attr_proj:
                                    for group_type in tree_group_type:
                                        fieldnames.append(attr.replace('.', '_')+'_'+group_type)
                            except Exception as e:
                                if not has_result:
                                    res = {
                                        'code' : 1, 
                                        'err_msg' : '数据格式错误'
                                    }
                                    has_error = True
                                    self.write(res)
                                    self.flush()
                                    self.finish()
                            if not has_error:
                                try:
                                    with open(('files/'+res_uuid+'.csv'), 'w', newline='') as f:
                                        writer = None
                                        # 储存根节点
                                        root_nodes = {}
                                        try:
                                            for doc in await cursor.to_list(length=CONFIG.TO_LIST_BUFFER_LENGTH):
                                                doc = json.loads(bson.json_util.dumps(doc))
                                                # 提取数据内相应的树的路径
                                                doc_tree_path = core.get_path_from_data(doc, tree_path_attr)
                                                if not doc_tree_path:
                                                    raise ValueError("路径不存在")
                                                # 如果这是第一条数据
                                                if not has_result:
                                                    # 如果这条数据有不为空的树的路径
                                                    if doc_tree_path:
                                                        res = {
                                                            'code' : 0,
                                                            'data' : {
                                                                'uuid' : res_uuid
                                                            }
                                                        }
                                                        self.write(res)
                                                        self.flush()
                                                        self.finish()
                                                        has_result = True
                                                        fieldnames += list(doc.keys())
                                                        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                                                        # 写入csv字段名称
                                                        writer.writeheader()
                                                        root_nodes[str(doc_tree_path[0])] = TreeNode(doc, doc_tree_path ,tree_attr_proj, show_raw_data)
                                                    # 如果制定的树的路径不存在，跳过数据
                                                    else:
                                                        pass
                                                # 如果不是第一条数据
                                                else:
                                                    # 如果这条数据有不为空的树的路径
                                                    if doc_tree_path:
                                                        # 如果根节点已经存在
                                                        if str(doc_tree_path[0]) in root_nodes:
                                                            root_nodes[str(doc_tree_path[0])].insert_data(doc, doc_tree_path, tree_attr_proj, show_raw_data)
                                                        # 如果根节点不存在
                                                        else:
                                                            root_nodes[str(doc_tree_path[0])] = TreeNode(doc, doc_tree_path ,tree_attr_proj, show_raw_data)
                                                    # 如果制定的树的路径不存在，跳过数据
                                                    else:
                                                        pass
                                            for root_key in root_nodes.keys():
                                                root_nodes[root_key].set_root()
                                                root_nodes[root_key].recursive_write_tree(writer)
                                        except Exception as e:
                                            if not has_result:
                                                res = {
                                                    'code' : 1, 
                                                    'err_msg' : '数据格式错误'
                                                }
                                                has_error = True
                                                self.write(res)
                                                self.flush()
                                                self.finish()
                                except Exception:
                                    # 罕见的24内出现两个UUID1相同情况
                                    pass
                                if not has_result:
                                    if not has_error:
                                        # HTTP响应内容
                                        res = {
                                            'code' : 5,
                                            'err_msg' : '搜索无结果'
                                        }
                                        self.write(res)
                                        self.flush()
                                        self.finish()
                        else:
                            res_uuid = helpers.get_UUID()
                            has_result = False
                            has_error = False
                            # 创建csv文件
                            with open(('files/'+res_uuid+'.csv'), 'w', newline='') as f:
                                fieldnames = []
                                writer = None
                                try:
                                    for doc in await cursor.to_list(length=CONFIG.TO_LIST_BUFFER_LENGTH):
                                        doc = json.loads(bson.json_util.dumps(doc))
                                        # 如果有符合条件的数据
                                        if not has_result:
                                            res = {
                                                'code' : 0,
                                                'data' : {
                                                    'uuid' : res_uuid
                                                }
                                            }
                                            self.write(res)
                                            self.flush()
                                            self.finish()
                                            has_result = True
                                            fieldnames = list(doc.keys())
                                            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                                            # 写入csv字段名称
                                            writer.writeheader()
                                        writer.writerow(doc)
                                except Exception:
                                    res = {
                                        'code' : 1, 
                                        'err_msg' : '数据格式错误'
                                    }
                                    has_error = True
                                    self.write(res)
                                    self.flush()
                                    self.finish()           
                            if not has_result:
                                if not has_error:
                                    # HTTP响应内容
                                    res = {
                                        'code' : 5,
                                        'err_msg' : '搜索无结果'
                                    }
                                    self.write(res)
                                    self.flush()
                                    self.finish()
                    else:
                        res = {
                            'code' : 1, 
                            'err_msg' : '数据格式错误'
                        }
                        self.write(res)
                        self.flush()
                        self.finish()
            else:
                res = {
                    'code' : 1, 
                    'err_msg' : '数据格式错误'
                }
                self.write(res)
                self.flush()
                self.finish()

def periodic_remove_old_file():
    """非阻塞，每一段时间清理一次过期csv查询文件

    非阻塞，每一段时间清理一次过期csv查询文件

    """
    def wrapper():
        remove_old_file()
        periodic_remove_old_file()
    scheduled_remove_old_file_timedelta = datetime.timedelta(hours=CONFIG.FILE_REMOVE_FREQUENCY_HOUR, \
            minutes=CONFIG.FILE_REMOVE_FREQUENCY_MINUTE, seconds=CONFIG.FILE_REMOVE_FREQUENCY_SECOND)
    # time.sleep会阻塞
    tornado.ioloop.IOLoop.current().add_timeout(scheduled_remove_old_file_timedelta, wrapper)

def remove_old_file():
    """非阻塞，每一段时间清理一次过期csv查询文件

    非阻塞，每一段时间清理一次过期csv查询文件
    
    """
    files_dir_path = os.path.join(os.path.dirname(__file__), 'files/')
    file_list = os.listdir(files_dir_path)
    has_print_msg = False
    # 删除过期的文件
    for file in file_list:
        cur_file_path = os.path.join(files_dir_path, file)
        file_modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(cur_file_path))
        if datetime.datetime.now() - file_modified_time > datetime.timedelta(hours=CONFIG.FILE_TTL_HOUR):
            try:
                os.remove(cur_file_path)
                # 打印清理的文件名
                if not has_print_msg:
                    print("删除过期的文件:")
                    has_print_msg = True
                print(file)
            except Exception:
                pass
    sys.stdout.flush()

def usage():
    """Usage信息

    打印Usage信息，-v 版本(如：1.0，float格式)，-p：端口，
    -f：强制覆盖归一值(更新版本LOG10_MAX值时)
    """
    print('Usage: main.py -v <version> [-p <port>] [-f] [-c] [-r]')

def main():
    """配置服务端，启用事件循环

    创建Tornado实例，配置Motor异步MongoDB连接库，HTTP请求路由，
    设置端口，启用事件循环

    """
    application_port = CONFIG.PORT
    application_version = None
    application_force_update = False
    application_clean_up_empty_combined_data = False
    application_remove_old_file = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'p:v:fcr', ['port=', 'version=', 'force', 'cleanup', 'remove'])
        for opt, arg in opts:
            if opt in ("-p", "--port"):
                application_port = int(arg)
            elif opt in ("-v", "--version"):
                application_version = float(arg)
                if application_version not in CONFIG.VERSION_LIST:
                    raise ValueError("输入的版本号不存在")
            elif opt in ("-f", "--force"):
                application_force_update = True
            elif opt in ("-c", "--cleanup"):
                application_clean_up_empty_combined_data = True
            elif opt in ("-r", "--remove"):
                application_remove_old_file = True
            else:
                pass
        if not application_version:
            print("请输入服务端版本号")
            usage()
            sys.exit()
    # 参数错误
    except getopt.GetoptError as err:
        print("参数错误: ", err)
        usage()
        sys.exit()
    # 输入的版本号不存在
    except ValueError as err:
        print(err)
        sys.exit()
    # 配置Motor异步MongoDB连接库
    db = motor.motor_tornado.MotorClient(CONFIG.DB_HOST, CONFIG.DB_PORT)[CONFIG.DB_NAME]
    application = tornado.web.Application([(r'/data', UploadHandler), \
                        (r'/search', SearchHandler), \
                        (r'/files/(.*)', tornado.web.StaticFileHandler, {"path" : './files'})], \
                        db=db, version=application_version)
    application.listen(application_port)
    app_ioloop = tornado.ioloop.IOLoop.current()

    # 处理更新版本，版本检查，清除无效合并数据，删除过期文件
    if application_force_update:
        app_ioloop.run_sync(lambda : core.update_norm_to_version(db, application_version))
    else:
        app_ioloop.run_sync(lambda : core.version_check(db, application_version))
    if application_clean_up_empty_combined_data:
        app_ioloop.run_sync(lambda : core.clean_up_empty_combined_data(db))
    if application_remove_old_file:
        app_ioloop.run_sync(remove_old_file)

    periodic_remove_old_file()

    print('Application running on port: ', application_port)
    sys.stdout.flush()
    # 启用非阻塞事件循环
    app_ioloop.start()

if __name__ == "__main__":
    main()