import os
import sys
import getopt
import operations
import shutil
from db import *

import os

class operation(operations.operation):
    "Initializes a test suite."
    def __init__(self):
        operations.operation.__init__(self,"init",False)
        self.add_argument("help",None,None,"show help message")
        self.add_argument("path","str",os.path.join(os.getcwd(),"ig_benchmark_test_suite"),"the path to create the root test suite")
        self.add_argument("verbose","int",0,"verbose level")
        pass

    def operate(self):
        if operations.operation.operate(self):
            errorMessage = None
            self.path = self.getSingleOption("path")
            source = os.path.join(os.path.dirname(os.path.abspath(__file__)),"basic_suite")
            try:
                shutil.copytree(source,self.path)
                self.mkdir(os.path.join(self.path,"working"))
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message = "{0}".format(exc_obj)
                self.error(message)
                return False
            if errorMessage == None:
                pass
            else:
                self.error(errorMessage)
                return False
            return True
