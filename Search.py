# python 3.6 模块
import json
import sys

# pip 安装模块
import bson.json_util
from bson.objectid import ObjectId
import numpy as np

# 本地文件，模块
from docs import conf as CONFIG

# 使用$in来匹配的attr
IN_ARRAY_MATCH_ATTR = ['rlist', 'pid', 'uid', 'fid', 'eid', 'name', 'tag', 'klist', 'cfg']

# 使用范围来匹配的attr
OR_RANGE_MATCH_ATTR = ['ugroup', 'exttype', 'type', 'v1', 'v2', 'utc_date']

# Search类，对象化的查询
class Search:
    def __init__(self, data):
        # Object
        self.data = data

        # String
        self.openid = data['openid']

        # Object
        self.extlist = self.get_attr('extlist')
        # Object
        self.v3 = self.get_attr('v3')
        # Object
        self.v3_upper = self.get_attr('v3_upper')


        # 使用 {'$in' : []} 匹配
        # 使用 self.query_in_array_match() 生成query

        # Object[]
        self.rlist = self.get_attr('rlist')
        # Object[]
        self.pid = self.get_attr('pid')
        # Object[]
        self.uid = self.get_attr('uid')
        # Object[]
        self.fid = self.get_attr('fid')
        # Object[]
        self.eid = self.get_attr('eid')
        # String[]
        self.name = self.get_attr('name')
        # Object[]
        self.tag = self.get_attr('tag')
        # Object[]
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
        # Object[]
        self.utc_date = self.get_attr('utc_date')
        # Object[]
        self.utc_date_upper = self.get_attr('date_upper')

        # 排序所需信息
        # String[]
        self.sort_order_by = self.get_attr('sort_order_by')
        # Number[]
        self.sort_asc = self.get_attr('sort_asc')

        # 聚合所需信息
        # String[]
        self.aggr_group_by = self.get_attr('aggr_group_by')
        # String[]
        self.aggr_attr_proj = self.get_attr('aggr_attr_proj')
        # String[]
        self.aggr_attr_group_type = self.get_attr('aggr_attr_group_type')

    def get_attr(self, attr_name):
        """从self.data读取相应的值
        
        如果attr_name存在，从self.data读取相应值

        参数：
            attr_name (String) : 需要提取的值的名称/key

        返回：
            返回类型由所需数据决定，attr_name不存在时返回None

        """
        return self.data[attr_name] if attr_name in self.data else None

    def query_openid(self):
        """生成query的openid部分

        提取openid，生成query匹配/match部分的openid条件
        openid属于匹配

        返回：
            Object[] : query匹配/match部分的openid条件

        """
        return [{'openid' : self.openid}]

    def query_extlist(self):
        """生成query的extlist部分

        提取extlist，生成query匹配/match部分的extlist条件
        extlist属于匹配

        返回：
            Object[] : query匹配/match部分的extlist条件，
                    搜索条件不存在extlist时返回[]

        """
        result = []
        if self.extlist:
            # 为extlist中每一组键值生成相应query
            for key in self.extlist.keys():
                result.append({('extlist.'+key) : {'$in' : self.extlist[key]}})
        return result

    def query_v3(self):
        """生成query的v3部分

        提取v3，生成query匹配/match部分的v3条件
        v3属于匹配和范围匹配

        返回：
            Object[] : query匹配/match部分的v3条件，
                    搜索条件不存在v3时返回[]

        """
        result = []
        if self.v3 and self.v3_upper:
            # 为v3中每一组键值生成相应query
            for key in self.v3.keys():
                or_query = []
                # v3的上限下限需同时存在
                if key in self.v3 and key in self.v3_upper:
                    # i为索引/index，val为i位上的值
                    for i, val in enumerate(self.v3[key]):
                        # 下限取自v3，上限取自v3_upper
                        lower_bound = val
                        upper_bound = self.v3_upper[key][i]
                        # 上限下限一样时为匹配情况
                        if lower_bound == upper_bound:
                            or_query += [{'v3.'+key : {'$eq' : lower_bound}}]
                        # 下限小于上限为范围匹配情况
                        elif lower_bound < upper_bound:
                            or_query += [{'v3.'+key : {'$gte' : lower_bound, '$lte' : upper_bound}}]
                        # 下限大于上限为单向范围匹配，如>=，<=
                        else:
                            # 匹配范围 x <= lower_bound
                            if upper_bound == -1:
                                or_query += [{'v3.'+key : {'$lte' : lower_bound}}]
                            # 匹配范围 x >= lower_bound
                            elif upper_bound == -2:
                                or_query += [{'v3.'+key : {'$gte' : lower_bound}}]
                            else:
                                pass  
                # 筛选符合任意一个范围或匹配值的数据
                if or_query:
                    result += [{'$or' : or_query}]
            # 不同的键值组成并列条件
            if result:
                result = [{'$and' : result}]
        return result

    def query_or_range_match(self, attr_name):
        """用于生成attr_name的范围匹配

        提取attr_name和attr_name_upper，生成匹配范围
        机制与生成query的v3部分相似

        返回：
            Object[] : query匹配/match部分的attr_name条件，
                    搜索条件不存在attr_name时返回[]

        """
        result = []
        # eval将String格式的变量名直接转化为变量
        if eval('self.'+attr_name) and eval('self.'+attr_name+'_upper'):
            # i为索引/index，val为i位上的值
            for i, val in enumerate(eval('self.'+attr_name)):
                # 下限取自attr_name，上限取自attr_name_upper
                lower_bound = val
                upper_bound = eval('self.'+attr_name+'_upper')[i]
                # 上限下限一样时为匹配情况
                if lower_bound == upper_bound:
                    result += [{attr_name : {'$eq' : lower_bound}}]
                # 下限小于上限为范围匹配情况
                elif lower_bound < upper_bound:
                    result += [{attr_name : {'$gte' : lower_bound, '$lte' : upper_bound}}]
                # 下限大于上限为单向范围匹配，如>=，<=
                else:
                    # 匹配范围 x <= lower_bound
                    if upper_bound == -1:
                        result += [{attr_name : {'$lte' : lower_bound}}]
                    # 匹配范围 x >= lower_bound
                    elif upper_bound == -2:
                        result += [{attr_name : {'$gte' : lower_bound}}]
                    else:
                        pass    
            # 筛选符合任意一个范围或匹配值的数据
            result = [{'$or' : result}]
        return result

    def query_in_array_match(self, attr_name):
        """用于生成attr_name的匹配

        提取attr_name，生成匹配

        返回：
            Object[] : query匹配/match部分的attr_name条件，
                    搜索条件不存在attr_name时返回[]

        """
        if eval('self.'+attr_name):
            return [{attr_name : {'$in' : eval('self.'+attr_name)}}]
        return []

    def query_match(self):
        """用于生成搜索query的match(筛选)部分

        生成query的match(筛选)部分，适用于搜索和聚合搜索

        返回：
            Object[] : query匹配/match部分的条件，
                    因为openid为必需，搜索条件不会返回[]

        """
        match = []
        # openid的筛选条件
        match += self.query_openid()
        # extlist的筛选条件
        match += self.query_extlist()
        # v3的筛选条件
        match += self.query_v3()
        # 所有匹配类型的筛选条件
        for attr in IN_ARRAY_MATCH_ATTR:
            match += self.query_in_array_match(attr)
        # 所有范围匹配类型的筛选条件
        for attr in OR_RANGE_MATCH_ATTR:
            match += self.query_or_range_match(attr)
        match += [{'v1' : {'$gt' : 0}}]
        match = {'$and' : match}
        return match

    def query_group(self):
        """用于生成聚合搜索query的group(聚合条件)部分

        生成query的group(聚合条件)部分，适用于聚合搜索

        返回：
            Object[] : query的group(聚合条件)部分的条件，
                    不存在聚合搜索条件时返回[]

        """
        if self.aggr_group_by and self.aggr_attr_proj and self.aggr_attr_group_type:
            result = {}
            # 聚合搜索里的_id指聚合所用的字段，可以有多个字段
            result['_id'] = {}
            # 生成group的聚合字段部分
            for group_by in self.aggr_group_by:
                result['_id'][group_by] = '$' + group_by
            # 生成group的结果字段(结果中需要显示的数值)部分
            for attr_proj in self.aggr_attr_proj:
                # 四种类型：max, min, avg, sum
                for attr_group_type in self.aggr_attr_group_type:
                    attr_proj_name = (attr_proj + '_' + attr_group_type).replace('.', '_')
                    result[attr_proj_name] = {('$'+attr_group_type) : ('$'+attr_proj)}
            return result
        return {}

    def query_sort(self):
        """用于生成搜索和聚合搜索query的sort(排序)部分

        生成query的sort(排序)部分，适用于搜索和聚合搜索

        返回：
            Object：query的group(聚合条件)部分的条件，
                    用于聚合搜索的情况，不存在排序条件时返回{}

            Tuple[] : query的group(聚合条件)部分的条件，
                    用于搜索的情况，不存在排序条件时返回[]

        """
        if self.sort_order_by and self.sort_asc:
            result = {}
            # Motor规定find里sort的格式为Tuple[]
            result_tuple = []
            # i为索引/index，val为i位上的值
            for i, attr in enumerate(self.sort_order_by):
                result[attr] = self.sort_asc[i]
                result_tuple += [(attr, self.sort_asc[i])]
            return result, result_tuple
        return {}, []

    def to_query(self, db):
        """用于生成搜索和聚合搜索的完整query

        生成搜索和聚合搜索的完整query，适用于搜索和聚合搜索

        返回：
            Object：query的group(聚合条件)部分的条件，
                    用于聚合搜索的情况，不存在排序条件时返回{}

            Tuple[] : query的group(聚合条件)部分的条件，
                    用于搜索的情况，不存在排序条件时返回[]

        """

          # query的match部分
        match_dict = self.query_match()
        # query的group部分
        group_dict = self.query_group()
        # query的sort部分
        sort_dict, sort_tuple = self.query_sort()
        # 聚合搜索的情况
        if group_dict:
            match = [{'$match' : match_dict}]
            group = [{'$group' : group_dict}]
            sort = [{'$sort' : sort_dict}] if sort_dict else []
            pipline = match + group + sort
            return db[CONFIG.COMBINED_COLLECTION_NAME].aggregate(pipline, allowDiskUse=True)
        # 非聚合搜索的情况
        else:
            # 需要排序时
            if sort_dict:
                return db[CONFIG.COMBINED_COLLECTION_NAME].find(match_dict).sort(sort_tuple)
            # 不需要排序时
            else:
                return db[CONFIG.COMBINED_COLLECTION_NAME].find(match_dict)


# TreeNode类，用于生成搜索(不适用于聚合搜索)结果的树状结构   
class TreeNode:
    def __init__(self, data, path, attr_proj, show_raw_data):
        """使用一条数据创建节点及符合数据的所有子节点

        创建节点后，通过self.insert_data更新当前节点统计数据，然后递归建立path上的子节点，
        直到创建完叶子节点(path长度为1)，叶子节点根据show_raw_data选择是否储存数据，
        叶子节点返回，被加入父级节点的self.children里，直到最后返回最开始创建的节点

        参数：
            data (Object[])：用于创建节点的数据
            path (Object[])：数据所经过的树的路径
            attr_proj (String[])：需要在结果中包括的值
            show_raw_data (Boolean)：是否保存合并数据(不保存则仅更新节点统计数据)

        """
        self.id = path[0]
        # 判定是否为叶子节点
        self.is_leaf = True if len(path) == 1 else False
        # 判定是否为根节点
        self.is_root = False
        # 统计数据
        self.summary = {}
        # 子节点，当当前节点为叶子节时为[]
        self.children = []
        # 储存的合并数据，当当前节点不是叶子节时为[]
        self.raw_data = []
        # 更新统计数据，递归更新统计数据/创建子节点
        self.insert_data(data, path, attr_proj, show_raw_data)

    def set_root(self):
        """将当前节点设为根节点

        将当前节点设为根节点

        """
        self.is_root = True

    def get_id(self):
        """返回当前节点的id(path的'$oid'键值)

        返回当前节点的id，用于判断子节点是否已经存在

        返回：
            Object：当前节点的id，{'$oid' : "000000000000000000000000"} 格式
        
        """
        return self.id

    def get_children(self):
        """返回当前节点的子节点Array

        返回当前节点的子节点Array

        返回：
            Object[]：子节点Array，叶子节点返回[]
        
        """
        return self.children

    def has_child(self, child_id):
        """检查当前节点是否包含子节点

        检查当前节点是否包含子节点，对比path和子节点id

        参数：
            child_id (Object)：需要查找的子节点id，{'$oid' : "000000000000000000000000"} 格式

        返回：
            Boolean：是否包含子节点
            Int：子节点在self.children中的index
        
        """
        for i, child in enumerate(self.children):
            if child.get_id() == child_id:
                return True, i
        return False, -1

    def is_leaf(self):
        """检查当前节点是否是叶子节点

        检查当前节点是否是叶子节点

        返回：
            Boolean：是否是叶子节点
        
        """
        return self.is_leaf

    def insert_data(self, data, path, attr_proj, show_raw_data):
        """在当前树形结构中插入数据

        更新当前节点统计数据，然后递归建立path上的子节点或使用子节点的insert_data，
        直到叶子节点(path长度为1)，叶子节点根据show_raw_data选择是否储存数据，
        叶子节点返回，被加入父级节点的self.children里，直至最后回到最初的insert_data函数

        参数：
            data (Object[])：需要插入的数据
            path (Object[])：数据所经过的树的路径
            attr_proj (String[])：需要在结果中包括的值
            show_raw_data (Boolean)：是否保存合并数据(不保存则仅更新节点统计数据)

        """
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
        """从数据中提取并扁平化结果中需要显示的数据

        从数据中提取并扁平化结果中需要显示的数据，
        辅助函数，用于更方便计算统计数据

        参数：
            data (Object[])：需要插入的数据
            attr_proj (String[])：需要在结果中包括的值

        返回：
            Object：扁平化过的数据

        """
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
        """计算当前节点统计数据

        根据数据计算或更新当前节点统计数据

        参数：
            data (Object[])：需要插入的数据
            attr_proj (String[])：需要在结果中包括的值

        """
        # 扁平化数据
        data_dict = self.filter_attr(data, attr_proj)
        # v1, v2, v3, v3_attr, v1_norm, v2_norm, v3_norm_attr
        for attr in data_dict.keys():
            # 更新已有统计数据
            if attr in self.summary:
                self.summary[attr]['count'] += 1
                self.summary[attr]['max'] = max(data_dict[attr], self.summary[attr]['max'])
                self.summary[attr]['min'] = min(data_dict[attr], self.summary[attr]['min'])
                self.summary[attr]['sum'] += data_dict[attr]
                self.summary[attr]['avg'] = self.summary[attr]['sum'] / self.summary[attr]['count']
            # 创建统计数据
            else:
                self.summary[attr] = {}
                self.summary[attr]['count'] = 1
                self.summary[attr]['max'] = data_dict[attr]
                self.summary[attr]['min'] = data_dict[attr]
                self.summary[attr]['sum'] = data_dict[attr]
                self.summary[attr]['avg'] = data_dict[attr]

    def recursive_write_tree(self, writer):
        """递归遍历树，将节点数据写入csv文件

        递归遍历树，将节点写入csv文件
        当前节点会先将当前节点统计数据写入csv文件，
        再将用子节点的recursive_write_tree写入子节点数据
        叶子节点将根据情况写入合并数据

        参数：
            writer (csv.DictWriter)：用于写入文件的writer，
                    根据header中字段自动写入dict中相应值

        """

        # 将统计数据扁平化
        self.flatten_summary()
        # 记录当前节点除统计数据外的信息
        self.summary['node_id'] = self.id
        self.summary['is_leaf'] = self.is_leaf
        self.summary['is_root'] = self.is_root
        children_list = []
        for child in self.children:
            children_list.append(child.get_id())
        self.summary['children'] = children_list
        # 将数据写入csv文件
        writer.writerow(self.summary)
        # 用子节点的recursive_write_tree写入子节点数据
        for child in self.children:
            child.recursive_write_tree(writer)
        # 如果存在合并数据，叶子节点写入合并数据
        if self.is_leaf:
            for data in self.raw_data:
                writer.writerow(data)

    def flatten_summary(self):
        """扁平化统计数据

        扁平化统计数据

        """
        tmp_summary = {}
        for attr in self.summary:
            for group_type in self.summary[attr].keys():
                tmp_summary[attr+'_'+group_type] = self.summary[attr][group_type]
        self.summary = tmp_summary

