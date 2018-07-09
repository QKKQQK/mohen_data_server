import numpy
import config as CONFIG

def log10_normalize(input):
	if input <= 1:
		return 0.0
	norm = numpy.log10(input) / numpy.log10(CONFIG.LOG10_MAX)
	return norm

def log10_add_normalize(a, b):
	return numpy.log10(1 + b/a) / numpy.log10(CONFIG.LOG10_MAX)

n = 10
n_norm = log10_normalize(n)

# $inc n 20
inc_n = 20
# n = 30
n += inc_n

# atomic operation returned val
returned_n = 10
n_norm_add = log10_add_normalize(10, 20)

inc_n = 30
n += inc_n

returned_n_2 = 30
n_norm_add_2 = log10_add_normalize(30, 30)

print(n_norm + n_norm_add + n_norm_add_2)
print(log10_normalize(n))

print(log10_normalize(20))