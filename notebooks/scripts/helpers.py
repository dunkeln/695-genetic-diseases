from functools import reduce

def compose(*fns):
    def __inner__(val):
        return reduce(lambda acc, fn: fn(acc), fns, val)
    return __inner__