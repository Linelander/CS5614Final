from StampMath import arithmetic
import operator

# y = StampMath.arithmetic(777, operator.add, 1, 7, 5)
# print("add (expect 13): " + str(y) + "\n")

# y = StampMath.arithmetic(777, operator.truediv, 6, 3, 5)
# print("true division (expect 0.4): " + str(y) + "\n")

# y = StampMath.arithmetic(777, operator.mul, 6, 3, 5)
# print("multiplication (expect 90): " + str(y) + "\n")

# y = StampMath.arithmetic(777, operator.sub, 6, 3, 5)
# print("subtraction (expect -2): " + str(y) + "\n")

# # Instantiate a stamped value
# three = StampMath.StampedValue(3)
# three.line_numbers = [17] # line number of creation. we'll automate this later

# # NOTE: probably need to get the line number where it's created too

# print("GENEALOGY TEST")
# print("--------------")
# a = StampMath.arithmetic(24, operator.sub, 6, 8, 5)
# print("integer subtraction (expect -7): " + str(a.value) + "\n")
# # a = -7

# b = StampMath.arithmetic(28, operator.sub, 3, a, 6)
# print("subtraction with stamped middle (expect 4): " + str(b.value) + "\n")
# # b = 4

# c = StampMath.arithmetic(32, operator.sub, b, 6, 5)
# print("subtraction 2 starting with stamped (expect -7): " + str(c.value) + "\n")
# # c = -7

# d = StampMath.arithmetic(36, operator.sub, 9, 7, c)
# print("subtraction with stamp at end (expect 9): " + str(d.value) + "\n")
# # d = 9

# print("Genealogy of stamped value named d: " + str(d.line_numbers))

# ## word count
# print('-'*5, 'word count', '-'*5)
# print(StampMath.getWordCount(d.line_numbers))

# # print("Genealogy of stamped value named d: " + str(d.line_numbers))

# a = 3
# b = 4
# c = arithmetic(operator.sub, 20, 10)
# x = arithmetic(operator.add, a, b, c)
# print(x.value, x.line_numbers)
