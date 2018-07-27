# python 3.6 模块
import json
import sys

# pip 安装模块
import bson.json_util
from bson.objectid import ObjectId
import numpy as np

# 本地文件，模块
from docs import conf as CONFIG

IN_ARRAY_MATCH_ATTR = ['rlist', 'pid', 'uid', 'fid', 'eid', 'name', 'tag', 'klist', 'cfg']
OR_RANGE_MATCH_ATTR = ['ugroup', 'exttype', 'type', 'v1', 'v2', 'utc_date']

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
        self.pid = self.get_attr('pid')
        # String[]
        self.uid = self.get_attr('pid')
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
        self.utc_date = self.get_attr('utc_date')
        # String[]
        self.utc_date_upper = self.get_attr('date_upper')

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
                or_query = []
                if key in self.v3 and key in self.v3_upper:
                    for i, val in enumerate(self.v3[key]):
                        lower_bound = val
                        upper_bound = self.v3_upper[key][i]
                        if lower_bound == upper_bound:
                            or_query += [{'v3.'+key : {'$eq' : lower_bound}}]
                        elif lower_bound < upper_bound:
                            or_query += [{'v3.'+key : {'$gte' : lower_bound, '$lte' : upper_bound}}]
                        else:
                            # 匹配范围 x <= lower_bound
                            if upper_bound == -1:
                                or_query += [{'v3.'+key : {'$lte' : lower_bound}}]
                            # 匹配范围 x >= lower_bound
                            elif upper_bound == -2:
                                or_query += [{'v3.'+key : {'$gte' : lower_bound}}]
                            else:
                                pass  
                if or_query:
                    result += [{'$or' : or_query}]
            if result:
                result = [{'$and' : result}]
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
    def __init__(self, data, path, attr_proj, show_raw_data):
        self.id = path[0]
        self.is_leaf = True if len(path) == 1 else False
        self.summary = {}
        self.children = []
        self.raw_data = []
        self.insert_data(data, path, attr_proj, show_raw_data)

    def get_id(self):
        return self.id

    def get_summary(self):
        return self.summary

    def get_children(self):
        return self.children

    def has_child(self, child_id):
        for i, child in enumerate(self.children):
            if child.get_id() == child_id:
                return True, i
        return False, -1

    def is_leaf(self):
        return self.is_leaf

    def insert_data(self, data, path, attr_proj, show_raw_data):
        # 更新当前节点数据统计信息
        self.calc_stats(data, attr_proj)
        # 当前节点是非叶子节点
        if not self.is_leaf:
            has_cur_child, index = self.has_child(path[1])
            # 如果path下一个节点存在于self.children中
            if has_cur_child:
                self.children[index].insert_data(data, path[1:], attr_proj, show_raw_data)
            # 如果不存在
            else:
                # 递归创建子节点，将子节点加入self.children
                child = TreeNode(data, path[1:], attr_proj, show_raw_data)
                self.children.append(child)
        # 当前节点是叶子节点
        else:
            if show_raw_data:
                self.raw_data.append(data)
     
    def filter_attr(self, data, attr_proj):
        result = {}
        for attr in attr_proj:
            # 如果attr为v3.attr或v3_norm.attr的格式
            if '.' in attr:
                attr_obj = attr.split('.', 1)[0]
                attr_key = attr.split('.', 1)[1]
                if attr_obj in data:
                    if attr_key in data[attr_obj]:
                        result[attr.replace('.', '_')] = data[attr_obj][attr_key]
            # 如果attr为v1, v2, v1_norm, v2_norm
            else:
                if attr in data:
                    result[attr] = data[attr]
        return result

    def calc_stats(self, data, attr_proj):
        data_dict = self.filter_attr(data, attr_proj)
        for attr in data_dict.keys():
            if attr in self.summary:
                self.summary[attr]['count'] += 1
                self.summary[attr]['max'] = max(data_dict[attr], self.summary[attr]['max'])
                self.summary[attr]['min'] = min(data_dict[attr], self.summary[attr]['min'])
                self.summary[attr]['sum'] += data_dict[attr]
                self.summary[attr]['avg'] = self.summary[attr]['sum'] / self.summary[attr]['count']
            else:
                self.summary[attr] = {}
                self.summary[attr]['count'] = 1
                self.summary[attr]['max'] = data_dict[attr]
                self.summary[attr]['min'] = data_dict[attr]
                self.summary[attr]['sum'] = data_dict[attr]
                self.summary[attr]['avg'] = data_dict[attr]

    def recursive_write_tree(self, writer):
    	self.flatten_summary()
    	self.summary['node_id'] = self.id
    	self.summary['is_leaf'] = self.is_leaf
    	children_list = []
    	for child in self.children:
    		children_list.append(child.get_id())
    	self.summary['children'] = children_list
    	writer.writerow(self.summary)
    	for child in self.children:
    		child.recursive_write_tree(writer)
    	if self.is_leaf:
    		for data in self.raw_data:
    			writer.writerow(data)

    def flatten_summary(self):
    	tmp_summary = {}
    	for attr in self.summary:
    		for group_type in self.summary[attr].keys():
    			tmp_summary[attr+'_'+group_type] = self.summary[attr][group_type]
    	self.summary = tmp_summary

