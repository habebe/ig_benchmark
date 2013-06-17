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
        self.add_argument("root","str",None,"Root path.")
        self.add_argument("verbose","int",0,"verbose level")
        self.builds = []
        pass


    def __run__(self,path,options):
        cwd = os.getcwd()
        os.chdir(path)
        exe = "ant"
        arguments = [exe] + options
        print "Run ",string.join(arguments)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
        p.wait()
        os.chdir(cwd)
        return True


    def __build__(self,path,project,template):
        parser = ig_template.Schema.Parser(template,path)
        parser.setVerboseLevel(self.verbose)
        parser.setProjectPath(path)
        parser.setProject(project)
        parser.parse()
        self.__run__(path,["clean"])
        self.__run__(path,["all"])
        return True
        
    def operate(self):
        if operations.operation.operate(self):
            self.root = self.getSingleOption("root")
            if self.root == None:
                self.error("Root path is not given.")
                return False
            self.root = os.path.abspath(self.root)
            template_path = os.path.join(self.root,"templates")
            working_path  = os.path.join(self.root,"working")
            listing = os.listdir(template_path)
            self.templates = []
            for i in listing:
                if i.endswith(".xml"):
                    project_item = i[:len(i)-4]
                    path_item = os.path.abspath(os.path.join(template_path,i))
                    working_item = os.path.join(working_path,project_item)
                    self.templates.append([path_item,project_item,working_item])
                    pass
                pass
            
            
            print working_path
            for i in self.templates:
                self.__build__(i[2],i[1],i[0])
                pass
            return True
