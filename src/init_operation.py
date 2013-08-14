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
        self.add_argument("path","str",os.getcwd(),"the path to create the root test suite")
        self.add_argument("verbose","int",0,"verbose level")
        pass

    def operate(self):
        if operations.operation.operate(self):
            errorMessage = None
            self.path = self.getSingleOption("path")
            source = os.path.join(os.path.dirname(os.path.abspath(__file__)),"basic_suite")
            print "shutil.copytree({0},{1})".format(source,self.path)
            listing = os.listdir(source)
            try:
                self.mkdir(os.path.join(self.path,"working"))
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message = "{0}".format(exc_obj)
                self.warn(message)
                pass
            for i in listing:
                try:
                    shutil.copytree(os.path.join(source,i),os.path.join(self.path,i))
                    pass
                except:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    message = "{0}".format(exc_obj)
                    self.warn(message)
                    pass
                pass
            if errorMessage == None:
                pass
            else:
                self.error(errorMessage)
                return False
            return True
