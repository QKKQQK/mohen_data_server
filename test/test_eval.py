class Search:
	def __init__(self, data):
		# String
		self.openid = data['openid']
		# String[]
		self.rlist = data['rlist'] if 'rlist' in data else None
		# Number[]
		self.ugroup = data['ugroup'] if 'ugroup' in data else None
		# Number[]
		self.ugroup_upper = data['ugroup_upper'] if 'ugroup_upper' in data else None

	def query_in_array_match(self, attr_name):
		if eval('self.'+attr_name):
			return [{attr_name : {'$in' : eval('self.'+attr_name)}}]
		return []

	def query_openid(self):
		return [{'openid' : self.openid}]

	def query_or_range_match(self, attr_name, attr_upper_name):
		result = []
		if eval('self.'+attr_name) and eval('self.'+attr_upper_name):
			for i, val in enumerate(eval('self.'+attr_name)):
				result += [{attr_name : {'$gte' : val, '$lte' : eval('self.'+attr_upper_name)[i]}}]
			result = [{'$or' : result}]
		return result

a = Search({'openid' : 123,
			'rlist' : ['a', 'b', 'c'],
			'ugroup' : [2010, 2018],
			'ugroup_upper' : [2012, 2018]})

print(a.query_in_array_match('rlist') + a.query_openid() + a.query_or_range_match('ugroup', 'ugroup_upper'))