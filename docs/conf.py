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
MIN_COLLECTION_NAME = 'tbl_report_min'

# 下载CSV时，每次传输 1 MB 数据
CHUNK_SIZE = 1024 * 512

# GET /data 返回数据数量限制
GET_LIMIT = 1000

# 归一化Log10最大值参数
LOG10_MAX = 300.0