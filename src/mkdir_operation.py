import os
import sys
import getopt
import operations
import shutil
from db import *
import ig_template
import os
import subprocess
import string
import config
import build_operation
import ig_property
import tempfile

class operation(operations.operation):
    "make directory operation: intended to be used for making paths on remote host."
    def __init__(self):
        operations.operation.__init__(self,"mkdir",False)
        self.add_argument("help",None,None,"show help message")
        self.add_argument("path","str",None,"the directory to create.")
        self.add_argument("verbose","int","0","Verbose level.")
        pass

    def operate(self):
        if operations.operation.operate(self):
            path = self.getSingleOption("path")
            self.verbose = self.getSingleOption("verbose")
            if path == None:
                self.error("Path is a required argument.")
                return False
            path = os.path.abspath(path)
            if not os.path.exists(path):
                paths = path.split(os.sep)
                currentPath = ""
                for i in paths:
                    i = i.strip()
                    if len(i) == 0:
                        i = os.sep
                        pass
                    currentPath = os.path.join(currentPath,i)
                    if not os.path.exists(currentPath):
                        if not self.mkdir(currentPath):
                            return False
                        pass
                    pass
                pass
            return True
        return False
    pass
