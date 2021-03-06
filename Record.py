# python 3.6 模块
import json

# pip 安装模块
import bson.json_util

# ObjectId类默认值
empty_id = {'$oid' : '000000000000000000000000'}

# RawRecord类，代表[原始数据]
class RawRecord:
    def __init__(self, data):
        """创建一个RawRecord实例

        参数：
            data (dict)：RawRecord实例的[原始数据]

        """
        self._id = data['_id']
        self.pid = data['pid'] if 'pid' in data else empty_id
        self.name = data['name'] if 'name' in data else ""
        self.flag = data['flag'] if 'flag' in data else 1
        self.exttype = data['exttype']
        self.type = data['type']
        self.tag = data['tag'] if 'tag' in data else []
        self.klist = data['klist'] if 'klist' in data else []
        self.rlist = data['rlist'] if 'rlist' in data else []
        self.extlist = data['extlist'] if 'extlist' in data else {}
        self.ugroup = data['ugroup'] if 'ugroup' in data else 0
        self.uid = data['uid'] if 'uid' in data else empty_id
        self.fid = data['fid'] if 'fid' in data else empty_id
        self.eid = data['eid']
        self.openid = data['openid']
        self.v1 = data['v1']
        self.v2 = data['v2'] if 'v2' in data else 0
        if 'v3' in data and data['v3']:
            self.v3 = data['v3']
            self.v3['placeholder'] = 0
        else:
            self.v3 = {'placeholder' : 0}
        self.cfg = data['cfg'] if 'cfg' in data else ""
        self.utc_date = data['utc_date']

    def to_json_str(self):
        """把实例转换成JSON字符串

        返回：
            String：当前实例转换成的JSON字符串

        """
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

    def to_bson(self):
        """把当前实例的JSON字符串转换成BSON格式的dict

        返回：
            dict：BSON格式的当前实例

        """
        return bson.json_util.loads(self.to_json_str())