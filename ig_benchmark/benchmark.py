#!/usr/bin/env python

import os
import sys
import getopt
import operations


def main():
    if len(sys.argv) < 2:
        operations.usage(__file__)
        sys.exit(1)
        pass
    operation_name = sys.argv[1].lower()
    operation = operations._operation_(operation_name)
    if operation == None:
        operations.usage(__file__)
    else:
        if operation.parse(sys.argv[2:]):
            operation.operate()
        sys.exit()
    pass

if __name__ == "__main__":
    main()
