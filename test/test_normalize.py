import numpy

def log10_normalize(input):
	if input <= 1:
		return 0.0
	norm = numpy.log10(input) / numpy.log10(300)
	return norm

def log10_add_normalize(a, b):
	if a == 0:
		return log10_normalize(b)
	return numpy.log10(1 + b/a) / numpy.log10(300)

v = 50
v_norm = log10_normalize(50)

v_2 = 70
v_2_norm = log10_normalize(v_2)

def log10_diff_normalize(v_old, v_new):
	"""计算归一值差值，适用于更新已有[原始数据]后需要更新相应的[合并数据]的情况
    
    基于公式：log(a + b) = log(a) + log(1 + b/a)
    当新值小于原值时，a为新值，b为(原值-新值)，返回负的归一值差值
    当新值大于原值时，a为原值，b为(新值-原值)，返回正的归一值差值
	当新值等于原值时，返回0

    参数：
        v_old (int/float)：数据的原值
        v_new (int/float)：数据的新值

    返回：
        float：计算后的归一值差值

	"""
	# 新值小于原值
	if v_new < v_old:
		return -log10_add_normalize(v_new, v_old - v_new)
	# 新值大于原值
	elif v_new > v_old:
		return log10_add_normalize(v_old, v_new - v_old)
	# 值不变
	else:
		return 0
print('v: ', v)
print('log10 v: ', v_norm)
print('v_2: ', v_2)
print('log10 v_2： ', v_2_norm)
print('log10 v_2 - log10 v: ', v_2_norm - v_norm)
print('log10 diff: ', log10_diff_normalize(v, v_2))

v_2 = 10
v_2_norm = log10_normalize(v_2)

print('v: ', v)
print('log10 v: ', v_norm)
print('v_2: ', v_2)
print('log10 v_2： ', v_2_norm)
print('log10 v_2 - log10 v: ', v_2_norm - v_norm)
print('log10 diff: ', log10_diff_normalize(v, v_2))
