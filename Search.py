# python 3.6 模块
import json
import sys

# pip 安装模块
import bson.json_util
from bson.objectid import ObjectId
import numpy as np

# 本地文件，模块
from docs import conf as CONFIG

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

        # String[]
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
                lower_bound = val
                upper_bound = eval('self.'+attr_name+'_upper')[i]
                if lower_bound == upper_bound:
                    result += [{attr_name : {'$eq' : lower_bound}}]
                elif lower_bound < upper_bound:
                    result += [{attr_name : {'$gte' : lower_bound, '$lte' : upper_bound}}]
                else:
                    # 匹配范围 x <= lower_bound
                    if upper_bound == -1:
                        result += [{attr_name : {'$lte' : lower_bound}}]
                    # 匹配范围 x >= lower_bound
                    elif upper_bound == -2:
                        result += [{attr_name : {'$gte' : lower_bound}}]
                    else:
                        pass    
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
        match += [{'v1' : {'$gt' : 0}}]
        match = {'$and' : match}
        return match

    def query_group(self):
        if self.aggr_group_by and self.aggr_attr_proj and self.aggr_attr_group_type:
            result = {}
            result['_id'] = {}
            for group_by in self.aggr_group_by:
            	result['_id'][group_by] = '$' + group_by
            for attr_proj in self.aggr_attr_proj:
                for attr_group_type in self.aggr_attr_group_type:
                    attr_proj_name = (attr_proj + '_' + attr_group_type).replace('.', '_')
                    result[attr_proj_name] = {('$'+attr_group_type) : ('$'+attr_proj)}
            return result
        return {}

    def query_sort(self):
        if self.sort_order_by and self.sort_asc:
            result = {}
            result_tuple = []
            for i, attr in enumerate(self.sort_order_by):
                result[attr] = self.sort_asc[i]
                result_tuple += [(attr, self.sort_asc[i])]
            return result, result_tuple
        return {}, []

    def to_query(self, db):
        match_dict = self.query_match()
        group_dict = self.query_group()
        sort_dict, sort_tuple = self.query_sort()
        if group_dict:
            match = [{'$match' : match_dict}]
            group = [{'$group' : group_dict}]
            sort = [{'$sort' : sort_dict}] if sort_dict else []
            pipline = match + group + sort
            return db[CONFIG.COMBINED_COLLECTION_NAME].aggregate(pipline, allowDiskUse=True)
        else:
            if sort_dict:
                return db[CONFIG.COMBINED_COLLECTION_NAME].find(match_dict).sort(sort_tuple)
            else:
                return db[CONFIG.COMBINED_COLLECTION_NAME].find(match_dict)
    
class TreeNode:
	def __init__(self, data):
		self.name = data['name']
		self.isleaf = data['isleaf']
		self.data = []
		self.children = []

	def get_name(self):
		return self.name

	def add_child(self, child):
		self.children.append(child)

	def is_leaf(self):
		return self.isleaf

	def add_data(self, data):
		self.data += [data]

	# def get_data(self, attr_proj, operations):
	# 	result = {}
	# 	if self.isleaf:
	# 		for attr in attr_proj:
	# 			attr_summary = {}
	# 			for opr in operations:
	# 				if opr == 'max'：
	# 					attr_summary['max'] = max(self.data[attr])
	# 				elif opr == 'min':
	# 					attr_summary['min'] = min(self.data[attr])
	# 				elif opr == 'avg':
	# 					attr_summary['avg'] = sum(self.data[attr]) / len(self.data[attr])
	# 				elif opr == 'sum':
	# 					attr_summary['sum'] = sum(self.data[attr])
	# 				else:
	# 					pass
	# 			attr_summary['count'] = len(self.data[attr])
	# 			result[attr] = attr_summary
	# 	else:
	# 		children_data = []
	# 		for child in self.children:
	# 			children_data += [child.get_data(attr_proj, operations)]
	# 		for attr in attr_proj:
	# 			attr_summary = {}
	# 			for opr in operations:
	# 				children_data_count = 0
	# 				for data in children_data:
	# 					children_data_count += data[attr]['count']
	# 				if opr == 'max'：
	# 					children_data_max = []
	# 					for data in children_data:
	# 						children_data_max += [data[attr]['max']]
	# 					attr_summary['max'] = max(children_data_max)
	# 				elif opr == 'min':
	# 					children_data_min = []
	# 					for data in children_data:
	# 						children_data_min += [data[attr]['min']]
	# 					attr_summary['min'] = min(children_data_min)
	# 				elif opr == 'avg':
	# 					children_data_sum = 0
	# 					for data in children_data:
	# 						children_data_sum += data[attr]['sum']
	# 					attr_summary['avg'] = children_data_sum / children_data_count
	# 				elif opr == 'sum':
	# 					children_data_sum = 0
	# 					for data in children_data:
	# 						children_data_sum += data[attr]['sum']
	# 					attr_summary['sum'] = children_data_sum
	# 				else:
	# 					pass
	# 			attr_summary['count'] = children_data_count
	# 			result[attr] = attr_summary
	# 	return result

