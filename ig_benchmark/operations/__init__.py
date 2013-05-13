import os
import sys
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

