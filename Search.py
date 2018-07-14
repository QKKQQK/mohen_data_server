# python 3.6 模块
import json

# pip 安装模块
import bson.json_util

IN_ARRAY_MATCH_ATTR = ['rlist', 'uid', 'fid', 'eid', 'name', 'tag', 'klist', 'cfg']
OR_RANGE_MATCH_ATTR = ['ugroup', 'exttype', 'type', 'v1', 'v2', 'date']

# Search类，代表查询
class Search:
	def __init__(self, data):
		# Dict
		self.data = data

		# String
		self.openid = data['openid']

		# Dict
		self.extlist = self.get_attr('extlist')
		# Dict
		self.v3 = self.get_attr('v3')
		# Dict
		self.v3_upper = self.get_attr('v3_upper')


		# 使用 {'$in' : []} 匹配
		# 使用 self.query_in_array_match() 生成query

		# String[]
		self.rlist = self.get_attr('rlist')
		# String[]
		self.uid = self.get_attr('uid')
		# String[]
		self.fid = self.get_attr('fid')
		# String[]
		self.eid = self.get_attr('eid')
		# String[]
		self.name = self.get_attr('name')
		# String[]
		self.tag = self.get_attr('tag')
		# String[]
		self.klist = self.get_attr('klist')
		# String[]
		self.cfg = self.get_attr('cfg')
		

		# 使用 {'$or' : [{'attr' : {'$gte' : val, '$lte' : val2}}, {'attr' : ...}]} 匹配
		# 使用 self.query_or_range_match() 生成query

		# Number[]
		self.ugroup = self.get_attr('ugroup')
		# Number[]
		self.ugroup_upper = self.get_attr('ugroup_upper')
		# Number[]
		self.exttype = self.get_attr('exttype')
		# Number[]
		self.exttype_upper = self.get_attr('exttype_upper')
		# Number[]
		self.type = self.get_attr('type')
		# Number[]
		self.type_upper = self.get_attr('type_upper')
		# Number[]
		self.v1 = self.get_attr('v1')
		# Number[]
		self.v1_upper = self.get_attr('v1_upper')
		# Number[]
		self.v2 = self.get_attr('v2')
		# Number[]
		self.v2_upper = self.get_attr('v2_upper')
		# String[]
		self.date = self.get_attr('date')
		# String[]
		self.date_upper = self.get_attr('date_upper')

		# 排序所需信息
		# Int[]
		self.sort_asc = self.get_attr('sort_asc')
		# String[]
		self.sort_order_by = self.get_attr('sort_order_by')

		# String
		self.aggr_group_by = self.get_attr('aggr_group_by')
		# String[]
		self.aggr_attr_proj = self.get_attr('aggr_attr_proj')
		# String[]
		self.aggr_attr_group_type = self.get_attr('aggr_attr_group_type')

	def get_attr(self, attr_name):
		return self.data[attr_name] if attr_name in self.data else None

	def query_openid(self):
		return [{'openid' : self.openid}]

	def query_extlist(self):
		result = []
		if self.extlist:
			for key in self.extlist.keys():
				result += [{('extlist.'+key) : {'$in' : self.extlist[key]}}]
		return result

	def query_v3(self):
		result = []
		if self.v3 and self.v3_upper:
			for key in self.v3.keys():
				result += [{('v3.'+key) : {'$gte' : self.v3[key], '$lte' : self.v3_upper[key]}}]
			result = [{'$or' : result}]
		return result

	def query_or_range_match(self, attr_name):
		result = []
		if eval('self.'+attr_name) and eval('self.'+attr_name+'_upper'):
			for i, val in enumerate(eval('self.'+attr_name)):
				result += [{attr_name : {'$gte' : val, '$lte' : eval('self.'+attr_name+'_upper')[i]}}]
			result = [{'$or' : result}]
		return result

	def query_in_array_match(self, attr_name):
		if eval('self.'+attr_name):
			return [{attr_name : {'$in' : eval('self.'+attr_name)}}]
		return []

	def query_match(self):
		match = []
		match += self.query_openid()
		match += self.query_extlist()
		match += self.query_v3()
		for attr in IN_ARRAY_MATCH_ATTR:
			match += self.query_in_array_match(attr)
		for attr in OR_RANGE_MATCH_ATTR:
			match += self.query_or_range_match(attr)
		return [{'$and' : match}]

	def query_group(self):
		if self.aggr_group_by and self.aggr_attr_proj and self.aggr_attr_group_type:
			result = {}
			result['_id'] = self.aggr_group_by
			for attr_proj in self.aggr_attr_proj:
				group_type = {}
				for attr_group_type in self.aggr_attr_group_type:
					group_type['$'+attr_group_type] = '$'+attr_proj
				result[attr_proj] = group_type
			return [result]

	def query_sort(self):
		if self.sort_order_by and self.sort_asc:
			result = {}
			for i, attr in enumerate(self.sort_order_by):
				result[attr] = self.sort_asc[i]
			return [result]

	




a = Search({'openid' : 123,
			'rlist' : ['a', 'b', 'c'],
			'ugroup' : [2010, 2018],
			'ugroup_upper' : [2012, 2018],
			'v3' : {'test_v3' : 123},
			'v3_upper' : {'test_v3' : 234},
			'aggr_group_by' : 'ugroup',
			'aggr_attr_proj' : ['v2', 'v3', 'v2_norm'],
			'aggr_attr_group_type' : ['sum', 'min']})

print(json.dumps(a.query_match(), indent=4))
print(a.query_sort())
print(a.query_group())