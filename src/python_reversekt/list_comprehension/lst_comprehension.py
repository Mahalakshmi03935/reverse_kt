squares = []  # create an empty list
for i in range(0, 11):  # creating a for loop
    squares.append(i ** 2)  # writing an expression
print(squares)
# list = [expression for item in iterable if conditional]
# create a new list with less syntax , like lambda function
# [expression (if/else) for item in iterable]

squares = [i ** 2 for i in range(0, 11)]
print(squares)

# using lambda function
student = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0]
passed_students = list(filter(lambda x: x >= 60, student))
print(passed_students)

# using list comprehension
passed = [i for i in student if i >= 60]
print(passed)

passup = [i if i >= 60 else "failed" for i in student]
print(passup)