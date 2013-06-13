import os
import sys


root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)




import base

def _operation_(name):
    return base._operation_(name)


def usage(fileName):
    base.populate()
    for i in base.operations:
        operation = base.operations[i]
        operation.usage(fileName)
        pass
    pass

