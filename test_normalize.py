import numpy

def log10_normalize(input):
	if input <= 1:
		return 0.0
	norm = numpy.log10(input) / numpy.log10(CONFIG.LOG10_MAX)
	return norm if norm < 1.0 else 1.0

print(numpy.log10(1) / numpy.log10(300))