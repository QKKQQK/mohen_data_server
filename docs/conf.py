# 已知服务器版本
VERSION_LIST = [1.0, 1.1]

# 归一化Log10最大值参数
LOG10_MAX = {
	'1.0' : 300,
	'1.1' : 100
}

# 服务器端口
PORT = 8080

# 数据库地址
DB_HOST = '127.0.0.1' 
# 数据库端口
DB_PORT = 27017
# 数据库名称
DB_NAME = 'test_report'

# 原始数据集合名称
RAW_COLLECTION_NAME = 'tbl_report_raw5'
# 合并数据集合名称
COMBINED_COLLECTION_NAME = 'tbl_report_min'

# 下载CSV时，每次传输 1 MB 数据
CHUNK_SIZE = 1024 * 512

# GET /data 返回数据数量限制
GET_LIMIT = 1000

# 查询时，每次I/O缓存文档条数
TO_LIST_BUFFER_LENGTH = 100

# 查询生成文件有效时长(小时)
FILE_TTL_HOUR = 24
