# 目录

* [数据库数据格式](#数据库数据格式)  
  * [原始数据 集合 文档结构](#原始数据-集合-文档结构)  
  * [原始数据 集合 字段要求(BSON)](#原始数据-集合-字段要求)  
* [API](#api)  
  * [POST 上传数据 JSON格式](#post-上传数据-json格式)
  * [POST 上传数据 字段要求](#post-上传数据-字段要求)
 
## 数据库数据格式

### 原始数据 集合 文档结构

    {
        "_id" : ObjectId,
        "openid" : String,
        "pid" : ObjectId,
        "name" : String,
        "flag" : Int32,
        "exttype" : Int32,
        "type" : Int32,
        "tag" : ObjectId[],
        "klist" : ObjectId[],
        "rlist" : ObjectId[],
        "extlist" : Object,
        "ugroup" : Int32,
        "uid" : ObjectId,
        "fid" : ObjectId,
        "eid" : ObjectId,
        "v1" : Double,
        "v2" : Double,
        "v3" : Object
        "cfg" : String,
        "utc_date" : Date
    }

### 原始数据 集合 字段要求  

字段 | 意义 | BSON类型 | 建立索引 | 例子
---- | ---- | --- | ---- | ----
_id  | 数据的MongoDB _id | ObjectId | 是 | ObjectId("5b3a62680000000000000000")  
openid | 数据提交第三方 _id, uuid格式 | String | 是  | "04ca7d4a-706e-11e8-adc0-fa7ae01bbebc"  
pid | 数据的父级节点 _id | ObjectId | 是 | ObjectId("5b3a62680000000000000000")  
name | 事件名称 | String | 是 | "文档查看"  
flag | 数据状态标志 | Int32 | 是 | 1  
exttype | 事件所属小类别代码 | Int32 | 是 | 512  
type | 事件所属大类别代码 | Int32 | 是 | 50  
tag | 事件相关标签(备用) | Array | 是 | [ObjectId("5b3a62680000000000000000"), ObjectId("5b3a62680000000000000001")]  
klist | 知识点树路径 | Array | 是 | [ObjectId("5b3a62680000000000000000"), ObjectId("5b3a62680000000000000001")]  
rlist | 关系树路径 | Array | 是 | [ObjectId("5b3a62680000000000000000"), ObjectId("5b3a62680000000000000001")]  
extlist | 拓展路径(备用) | Object | 是  (例子：extlist, extlist.test_path) | {test_path : [ObjectId("5b3a62680000000000000000"), ObjectId("5b3a62680000000000000001")]}  
ugroup | 用户所属大分类(如“届”)代码 | Int32 | 是 | 2016  
uid | 用户 _id | ObjectId | 是 | ObjectId("5b3a62680000000000000000")  
fid | 文件 _id(备用) | ObjectId | 是 | ObjectId("5b3a62680000000000000000")  
eid | 设备 _id | ObjectId | 是 | ObjectId("5b3a62680000000000000000")  
v1 | 数值：操作次数 | Double | 是 | 10.0  
v2 | 数值：事件时长 | Double | 是 | 10.0  
v3 | 拓展数值(备用) | Object | 是  (例子：v3, v3.test_val1, v3.test_val2) | {test_val1 : 10.0, test_val2 : 9999.0}  
cfg | 字符串值 | String | 是 | "Y\|Y\|Y\|"  
utc_date | 数据创建日期时间(UTC+0) | Date | 是 | "2018-06-12 10:53:54.247"  

## API  

### POST 上传数据 JSON格式
#### 路径: /data

#### 例子

    {
        "data" : [{
            "_id" : {"$oid" : "5a0ab7dad5cb310b9830ef27"},  
            "openid" : "f857e9f6-6e26-11e8-adc0-fa7ae01bbebc",  
            "pid"  : {"$oid" : "5a0ab7dad5cb310b9830ef27"},  
            "name" : "密码重置",
            "flag" : 1,
            "exttype" : 512,  
            "type" : 50,  
            "tag": [{"$oid" : "5a0ab7dad5cb310b9830ef26"}, {"$oid" : "5a0ab7dad5cb310b9830ef27"}],  
            "klist" : [{"$oid" : "5a0ab7dad5cb310b9830ef26"}, {"$oid" : "5a0ab7dad5cb310b9830ef27"}],  
            "rlist" : [{"$oid" : "5a0ab7dad5cb310b9830ef26"}, {"$oid" : "5a0ab7dad5cb310b9830ef27"}], 
            "extlist" : { "path_1" : [{"$oid" : "5a0ab7dad5cb310b9830ef26"}, {"$oid" : "5a0ab7dad5cb310b9830ef27"}] },  
            "ugroup" : 2015,  
            "uid" : {"$oid" : "5a0ab7dad5cb310b9830ef27"},  
            "fid" : {"$oid" : "5a0ab7dad5cb310b9830ef27"},  
            "eid" : {"$oid" : "5a0ab7dad5cb310b9830ef27"},  
            "v1" : 10.0,  
            "v2" : 15.0,  
            "v3" : {  "val_1" : 123.456 },  
            "cfg" : "Y|Y|Y|",  
            "utc_date" : {"$date" : 1530010000000}
        }, {
            ...
        }, {
            ...
        }]
    }

### POST 上传数据 字段要求  

字段 | 意义 | 必需 | 类型 |  要求 | 默认值 | 例子  
---- | ---- | ---- | ---- | ---- | ----- | ----- 
_id  | 数据的MongoDB _id | 是 | Object | 24位16进制数字字符串，不可重复 | 无 | {"$oid" : "5a0ab7dad5cb310b9830ef27"}  
openid | 数据提交第三方 _id | 是 | String | 使用UUID1 | 无 | "f857e9f6-6e26-11e8-adc0-fa7ae01bbebc"  
pid  | 数据的父级节点 _id | 否 | Object | 24位16进制数字字符串 | {"$oid" : "000000000000000000000000"} | {"$oid" : "5a0ab7dad5cb310b9830ef27"}  
name | 事件名称 | 否 | String | 无 | "" | "密码重置"   
flag | 数据有效性 | 否 | Number | 1为有效，0为无效(被删除，不会显示在搜索结果里但保留在数据库中) | 1 | 1  
exttype | 事件所属小类别代码 | 是 | Number | 无 | 无 | 512  
type | 事件所属大类别代码 | 是 | Number | 无 | 无 |50  
tag | 事件相关标签(备用) | 否 | Object[] | 24位16进制数字字符串 | [] | [{"$oid" : "5a0ab7dad5cb310b9830ef26"}, {"$oid" : "5a0ab7dad5cb310b9830ef27"}]  
klist | 知识点树路径 | 否 | Object[] | 24位16进制数字字符串 | [] | [{"$oid" : "5a0ab7dad5cb310b9830ef26"}, {"$oid" : "5a0ab7dad5cb310b9830ef27"}]  
rlist | 关系树路径 | 否 | Object[] | 24位16进制数字字符串 | [] | [{"$oid" : "5a0ab7dad5cb310b9830ef26"}, {"$oid" : "5a0ab7dad5cb310b9830ef27"}]  
extlist | 拓展路径(备用) | 否 | Object | 24位16进制数字字符串 | {} | {"path_1" : [{"$oid" : "5a0ab7dad5cb310b9830ef26"}, {"$oid" : "5a0ab7dad5cb310b9830ef27"}]}  
ugroup | 用户所属大分类(如“届”)代码 | 否 | Number | 无 | 0 | 2015  
uid | 用户 _id | 否 | Object | 24位16进制数字字符串 | {"$oid" : "000000000000000000000000"} | {"$oid" : "5a0ab7dad5cb310b9830ef27"}  
fid | 文件 _id(备用) | 否 | Object | 24位16进制数字字符串 | {"$oid" : "000000000000000000000000"} | {"$oid" : "5a0ab7dad5cb310b9830ef27"}  
eid | 设备 _id | 是 | Object | 24位16进制数字字符串 | 无 | {"$oid" : "5a0ab7dad5cb310b9830ef27"}  
v1 | 数值：操作次数 | 是 | Number | v1必须大于0 | 无 | 10.0  
v2 | 数值：事件时长 | 否 | Number | 无 | 0 | 15.0  
v3 | 拓展数值(备用) | 否 | Object | 仅用于存储Number类，不可用于储存Object类 | {} | {"val_1" : 123.456}  
cfg | 字符串值 | 否 | String | 无 | "" | "Y\|Y\|Y\|"  
utc_date | 数据时间戳 | 是 | Object | 时间戳(毫秒)，无需考虑时区 | 无 | {"$date" : "1530010000000"}  

[返回目录](#目录)
