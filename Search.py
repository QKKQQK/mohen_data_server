# python 3.6 模块
import json

# pip 安装模块
import bson.json_util

# Search类，代表查询
class Search:
	def __init__(self, data):
		self.openid = data['openid']
		self.rlist = data['rlist'] if 'rlist' in data else None
		self.extlist = data['extlist'] if 'extlist' in data else None
		self.ugroup = data['ugroup'] if 'ugroup' in data else None
		self.uid = data['uid'] if 'uid' in data else None
		self.fid = data['fid'] if 'fid' in data else None
		self.eid = data['eid'] if 'eid' in data else None
		self.name = data['name'] if 'name' in data else None
		self.exttype = data['exttype'] if 'exttype' in data None
		self.type = data['type'] if 'type' in data else None
		self.tag = data['tag'] if 'tag' in data else None
		self.klist = data['klist'] if 'klist' in data else None
		self.date = data['date'] if 'date' in data else None
		self.v1 = data['v1'] if 'v1' in data else None
		self.v2 = data['v2'] if 'v2' in data else None
		self.v3 = data['v3'] if 'v3' in data else None
		self.cfg = data['cfg'] if 'cfg' in data else None

		self.ugroup_upper = data['ugroup_upper'] if 'ugroup_upper' in data else None
		self.exttype_upper = data['exttype_upper'] if 'exttype_upper' in data else None
		self.type_upper = data['type_upper'] if 'type_upper' in data else None
		self.date_upper = data['date_upper'] if 'date_upper' in data else None
		self.v1_upper = data['v1_upper'] if 'v1_upper' in data else None
		self.v2_upper = data['v2_upper'] if 'v2_upper' in data else None
		self.v3_upper = data['v3_upper'] if 'v3_upper' in data else None

		self.asc = data['asc'] if 'asc' in data else 1
		self.order_by = data['order_by'] if 'order_by' in data else None

		self.group_type = data['group_type'] if 'group_type' in data else None
		self.group_by = data['group_by'] if 'group_by' in data else None
		self.results = data['results'] if 'results' in data else None

