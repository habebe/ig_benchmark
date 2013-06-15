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

class operation(operations.operation):
    "Bootstrap the Graph database given a template and a configuration file."
    def __init__(self):
        operations.operation.__init__(self,"bootstrap",False)
        self.add_argument("help",None,None,"show help message")
        self.add_argument("root","str",None,"Root path.")
        self.add_argument("config","str",None,"specify the config file name and the name of the config using the format config_file_name:config_name.")
        self.add_argument("project","str",None,"name of the project.")
        
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
            rootPath = self.getSingleOption("root")
            configParameter = self.getSingleOption("config")
            self.project = self.getSingleOption("project")
            if self.project == None:
                self.error("Project is not given.")
                return False
            working_path = os.path.join(rootPath,"working")
            project_path = os.path.join(working_path,self.project)
            if not os.path.exists(project_path):
                self.warn("Project path '{0}' does not exist. Trying to build project.".format(self.project))
                build = build_operation.operation()
                build.parse(["--root","{0}".format(rootPath)])
                build.operate()
                if not os.path.exists(project_path):
                    self.warn("Project path '{0}' does not exist after an attempted build.".format(self.project))
                    return False
                pass
            if not configParameter:
                self.error("Config Parameter is not given.")
                return False
            configParameter = configParameter.split(":")
          
            self.configFileName = configParameter[0]
            
            if not self.configFileName.endswith(".xml"):
                self.configFileName = self.configFileName + ".xml"
                pass
            self.configName = None
            if len(configParameter) == 2:
                self.configName = configParameter[1]
                pass
            if rootPath:
                self.configFileName = os.path.join(rootPath,"config",self.configFileName)
                pass
            try:
                configList = config.parse(self.configFileName,self)
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                self.error("Error while parsing config file. {0}".format(exc_obj))
                return False
            self.config = None
            if configList and len(configList.configs) > 0:
                if self.configName:
                    for i in configList.configs:
                        if i.name == self.configName:
                            self.config = i
                            pass
                        pass
                    if self.config == None:
                        self.error("Unable to find config with a name {0}.".format(self.configName))
                        return False
                    pass
                else:
                    self.config = configList.configs[0]
                    pass
                print self.output_string("Bootstrap using\n{0}".format(self.config),True,True)
                return True
            else:
                self.error("Error processing config file.")
                return False
            return False
        return False
