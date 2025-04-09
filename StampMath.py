import operator


class StampedValue:
    def __init__(self, val, loc):
        self.value = val
        self.line_number = loc





def stamp_math(lineNo, operation, *args):
    if type(args[0]) == StampedValue:
        x = args[0].value
    else:
        x = args[0]
    i = 1
    while i < len(args):
        if type(args[i]) == StampedValue:
            x = operation(x, args[i].value)
        else:
            x = operation(x, args[i])
        i+=1
    return x


y = stamp_math(777, operator.add, 1, 7, 5)
print("add (expect 13): " + str(y) + "\n")

y = stamp_math(777, operator.truediv, 6, 3, 5)
print("true division (expect 0.4): " + str(y) + "\n")

y = stamp_math(777, operator.mul, 6, 3, 5)
print("multiplication (expect 90): " + str(y) + "\n")

y = stamp_math(777, operator.sub, 6, 3, 5)
print("subtraction (expect -2): " + str(y) + "\n")

three = StampedValue(3, -1)

y = stamp_math(777, operator.sub, 6, three, 5)
print("subtraction with stamped middle (expect -2): " + str(y) + "\n")

y = stamp_math(777, operator.sub, 3, 6, 5)
print("subtraction 2 (expect -8): " + str(y) + "\n")

y = stamp_math(777, operator.sub, three, 6, 5)
print("subtraction 2 starting with stamped (expect -8): " + str(y) + "\n")

y = stamp_math(777, operator.sub, three, three, three)
print("subtraction on only stamped numbers (expect -3): " + str(y) + "\n")