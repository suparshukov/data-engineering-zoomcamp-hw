def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1


# Question 1: What is the sum of the outputs of the generator for limit = 5?
# limit = 5
# generator = square_root_generator(limit)
# sum = 0
# for sqrt_value in generator:
#     # print(sqrt_value)
#     sum += sqrt_value
# print(sum)


# Question 2: What is the 13th number yielded
limit = 13
generator = square_root_generator(limit)

sum = 0
i = 1
for sqrt_value in generator:
    print(i, sqrt_value)
    sum += sqrt_value
    i += 1
print(sum)
