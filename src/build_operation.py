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
import tempfile

class operation(operations.operation):
    "Build a template"
    def __init__(self):
        operations.operation.__init__(self,"build",False)
        self.add_argument("help",None,None,"show help message.")
        self.add_argument("root","str",None,"Root path.")
        self.add_argument("verbose","int",0,"verbose level.")
        self.add_argument("ig_version","str",None,"InfiniteGraph version.")
        self.add_argument("ig_home","str",None,"InfiniteGraph installation path.")
        self.add_argument("verbose","int","0","Verbose level.")
        self.builds = []
        pass


    def __run__(self,path,options,showIG_HOME=False):
        cwd = os.getcwd()
        os.chdir(path)
        exe = "ant"
        arguments = [exe] + options
        env = os.environ.copy()
        if platform.system().lower().find("darwin") >= 0:
            env["IG_HOME"] = os.path.join(self.ig_home,"mac86_64")
            pass
        elif platform.system().lower().find("linux") >= 0:
            env["IG_HOME"] = os.path.join(self.ig_home,"linux86_64")
            pass
        if self.verbose == 0:
            if showIG_HOME:
                print "IG_HOME:{0}".format(env["IG_HOME"]),
                sys.stdout.flush()
                pass
            pass
        status = False
        try:
            stdout = sys.stdout
            if self.verbose == 0:
                stdout = tempfile.TemporaryFile()
                pass  
            p = subprocess.Popen(arguments,stdout=stdout,stderr=sys.stderr,env=env)
            p.wait()
            status = (p.returncode == 0)
        except:
            status = False
            exc_type, exc_obj, exc_tb = sys.exc_info()
            message = "Error when building project.\n"
            message += "{0}\n".format(string.join(arguments))
            message += str(exc_obj)
            self.error(message)
            pass
        os.chdir(cwd)
        return status


    def __build__(self,path,project,template):
        parser = ig_template.Schema.Parser(template,path)
        parser.setVerboseLevel(self.verbose)
        parser.setProjectPath(path)
        parser.setProject(project)
        parser.parse()
        self.__run__(path,["clean"],True)
        self.__run__(path,["all"])
        return True
        
    def operate(self):
        if operations.operation.operate(self):
            self.root    = self.getSingleOption("root")
            self.ig_version = self.getSingleOption("ig_version")
            self.ig_home  = self.getSingleOption("ig_home")
            self.verbose = self.getSingleOption("verbose")
            if self.verbose == 0:
                print "\t\tBuilding Templates",
                sys.stdout.flush()
                pass
            
            if self.root == None:
                self.error("Root path is not given.")
                return False

            if self.ig_version == None:
                self.error("InfiniteGraph version is not given.")
                return False

            if self.ig_home == None:
                self.error("InfiniteGraph installation path is not given.")
                return False
           
            
            self.root = os.path.abspath(self.root)
            template_path = os.path.join(self.root,"templates")
            working_path  = os.path.join(self.root,"working",self.ig_version)

            if self.verbose == 0:
                print "path:{0}".format(working_path),
                sys.stdout.flush()
                pass
            
            if not os.path.exists(working_path):
                os.mkdir(working_path)
                pass
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

            for i in self.templates:
                if not self.__build__(i[2],i[1],i[0]):
                    return False
                pass
            if self.verbose == 0:
                print "[complete]"
                pass
            return True
        return False
