def fibonacci_series(count):
	"""Return the first `count` Fibonacci numbers."""
	if count < 0:
		raise ValueError("count must be non-negative")

	numbers = []
	first, second = 0, 1
	for _ in range(count):
		numbers.append(first)
		first, second = second, first + second
	return numbers
git 