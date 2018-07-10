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
	if v_new - v_old < 0:
		return -log10_add_normalize(v_new, v_old - v_new)
	elif v_new - v_old > 0:
		return log10_add_normalize(v_old, v_new - v_old)
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
