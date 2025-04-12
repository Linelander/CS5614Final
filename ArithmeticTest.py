import StampMath
import operator

y = StampMath.arithmetic(777, operator.add, 1, 7, 5)
print("add (expect 13): " + str(y) + "\n")

y = StampMath.arithmetic(777, operator.truediv, 6, 3, 5)
print("true division (expect 0.4): " + str(y) + "\n")

y = StampMath.arithmetic(777, operator.mul, 6, 3, 5)
print("multiplication (expect 90): " + str(y) + "\n")

y = StampMath.arithmetic(777, operator.sub, 6, 3, 5)
print("subtraction (expect -2): " + str(y) + "\n")

# Instantiate a stamped value
three = StampMath.StampedValue(3)
three.line_numbers = [17] # line number of creation. we'll automate this later

# NOTE: probably need to get the line number where it's created too

print("GENEALOGY TEST")
print("--------------")
a = StampMath.arithmetic(24, operator.sub, 6, 8, 5)
print("integer subtraction (expect -7): " + str(a.value) + "\n")
# a = -7

b = StampMath.arithmetic(28, operator.sub, 3, a, 6)
print("subtraction with stamped middle (expect 4): " + str(b.value) + "\n")
# b = 4

c = StampMath.arithmetic(32, operator.sub, b, 6, 5)
print("subtraction 2 starting with stamped (expect -7): " + str(c.value) + "\n")
# c = -7

d = StampMath.arithmetic(36, operator.sub, 9, 7, c)
print("subtraction with stamp at end (expect 9): " + str(d.value) + "\n")
# d = 9

print("Genealogy of stamped value named d: " + str(d.line_numbers))

## string stamp
print('-'*5, 'string stamp', '-'*5)
s1 = StampMath.StampedValue('Hello')
s2 = StampMath.StampedValue(' ')
firstWord = StampMath.string_concat(46, s1, s2)
s3 = StampMath.StampedValue('World')
s4 = StampMath.StampedValue('!')
secondWord = StampMath.string_concat(49, s3, s4)
whole_string = StampMath.string_concat(50, firstWord, secondWord)
print(whole_string.value, whole_string.line_numbers)