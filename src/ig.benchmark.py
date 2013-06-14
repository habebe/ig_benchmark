#!/usr/bin/env python

import os
import sys
import getopt
import ig_benchmark

def main():
    if len(sys.argv) < 2:
        ig_benchmark.usage(__file__)
        sys.exit(1)
        pass
    operation_name = sys.argv[1].lower()
    operation = ig_benchmark.operations._operation_(operation_name)
    if operation == None:
        print "Error: Unknown operation type '{0}'".format(operation_name)
        ig_benchmark.usage(__file__)
    else:
        if operation.parse(sys.argv[2:]):
            operation.operate()
        sys.exit()
    pass

if __name__ == "__main__":
    main()
