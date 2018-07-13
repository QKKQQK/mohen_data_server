# python 3.6 模块
import json

# pip 安装模块
import bson.json_util

# Search类，代表查询
class Search:
	def __init__(self, data):
		# String
		self.openid = data['openid']
		# String[]
		self.rlist = data['rlist'] if 'rlist' in data else None
		# Dict
		self.extlist = data['extlist'] if 'extlist' in data else None
		# Number[]
		self.ugroup = data['ugroup'] if 'ugroup' in data else None
		# Number[]
		self.ugroup_upper = data['ugroup_upper'] if 'ugroup_upper' in data else None
		# String[]
		self.uid = data['uid'] if 'uid' in data else None
		# String[]
		self.fid = data['fid'] if 'fid' in data else None
		# String[]
		self.eid = data['eid'] if 'eid' in data else None
		# String[]
		self.name = data['name'] if 'name' in data else None
		# Number[]
		self.exttype = data['exttype'] if 'exttype' in data None
		# Number[]
		self.exttype_upper = data['exttype_upper'] if 'exttype_upper' in data else None
		# Number[]
		self.type = data['type'] if 'type' in data else None
		# Number[]
		self.type_upper = data['type_upper'] if 'type_upper' in data else None
		# String[]
		self.tag = data['tag'] if 'tag' in data else None
		# String[]
		self.klist = data['klist'] if 'klist' in data else None
		# String[]
		self.date = data['date'] if 'date' in data else None
		# String[]
		self.date_upper = data['date_upper'] if 'date_upper' in data else None
		# Number[]
		self.v1 = data['v1'] if 'v1' in data else None
		# Number[]
		self.v1_upper = data['v1_upper'] if 'v1_upper' in data else None
		# Number[]
		self.v2 = data['v2'] if 'v2' in data else None
		# Number[]
		self.v2_upper = data['v2_upper'] if 'v2_upper' in data else None
		# Dict
		self.v3 = data['v3'] if 'v3' in data else None
		# Dict
		self.v3_upper = data['v3_upper'] if 'v3_upper' in data else None
		# String
		self.cfg = data['cfg'] if 'cfg' in data else None

		# Int
		self.asc = data['asc'] if 'asc' in data else 1
		# String
		self.order_by = data['order_by'] if 'order_by' in data else None

		# String[]
		self.group_type = data['group_type'] if 'group_type' in data else None
		# String
		self.group_by = data['group_by'] if 'group_by' in data else None
		# String[]
		self.results = data['results'] if 'results' in data else None

	def query_openid(self):
		return [{'openid' : self.openid}]

	def query_extlist(self):
		result = []
		if self.extlist:
			for key in self.extlist.keys():
				result += [{('extlist.'+key) : {'$in' : self.extlist[key]}}]
		return result

	def or_query_insert_ugroup(self):
		result = []
		if self.ugroup and self.ugroup_upper:
			for i, val in enumerate(self.ugroup):
				result += [{'ugroup' : {'$gte' : val, '$lte' : self.ugroup_upper[i]}}]
			result = {'$or' : result}
		return result

	def query_or_range_match(self, attr_name, attr_upper_name):
		result = []
		if eval('self.'+attr_name) and eval('self.'+attr_upper_name):
			for i, val in enumerate(eval('self.'+attr_name)):
				result += [{attr_name : {'$gte' : val, '$lte' : eval('self.'+attr_upper_name)[i]}}]
			result = {'$or' : result}
		return result

	def query_in_array_match(self, attr_name):
		if eval('self.'+attr_name):
			return [{attr_name : {'$in' : eval('self.'+attr_name)}}]
		return []
