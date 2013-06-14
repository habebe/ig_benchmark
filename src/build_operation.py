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

class operation(operations.operation):
    "Build a template"
    def __init__(self):
        operations.operation.__init__(self,"build",False)
        self.add_argument("help",None,None,"show help message")
        self.add_argument("path","str",None,"the path to create where the template will be generated.")
        self.add_argument("project","str",None,"name of the project.")
        self.add_argument("template","str",None,"the name of the template file.")
        self.add_argument("verbose","int",0,"verbose level")
        pass


    def run(self,path,options):
        cwd = os.getcwd()
        os.chdir(path)
        exe = "ant"
        arguments = [exe] + options
        print "Run ",string.join(arguments)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
        p.wait()
        os.chdir(cwd)
        return True


    def operate(self):
        if operations.operation.operate(self):
            errorMessage = None
            self.path = self.getSingleOption("path")
            self.project = self.getSingleOption("project")
            self.template = self.getSingleOption("template")
            if self.path == None:
                self.error("Path is not given.")
                return False

            if self.template == None:
                self.error("Template file is not given.")
                return False
            
            if self.project == None:
                self.error("Project is not given.")
                return False
            
            parser = ig_template.Schema.Parser(self.template,self.path)
            parser.setVerboseLevel(self.verbose)
            parser.setProjectPath(self.path)
            parser.setProject(self.project)
            parser.parse()

            self.run(self.path,["clean"])
            self.run(self.path,["all"])
            
            if errorMessage == None:
                pass
            else:
                self.error(errorMessage)
                return False
            return True
