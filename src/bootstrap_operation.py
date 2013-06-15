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

class operation(operations.operation):
    "Bootstrap the Graph database given a template and a configuration file."
    def __init__(self):
        operations.operation.__init__(self,"bootstrap",False)
        self.add_argument("help",None,None,"show help message")
        self.add_argument("root","str",None,"Root path.")
        self.add_argument("config","str",None,"specify the config file name and the name of the config using the format config_file_name:config_name.")
        self.add_argument("project","str",None,"name of the project.")
        self.add_argument("page_size","str",None,"page size to use.")
        self.add_argument("no_index",None,None,"Do not create index.")
        self.add_argument("containers","int","0","Create containers with this size.")
        pass


    def createGraph(self,project,propertyFile,noIndex):
        cwd = os.getcwd()
        os.chdir(self.working_path)
        arguments = ["java","-jar",os.path.join(self.working_path,project,"build","bootstrap.jar"),"-overwrite","-property",propertyFile.fileName]
        if noIndex:
            arguments.append("-no_index")
        self.output_string("\t"+string.join(arguments),True,False)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
        p.wait()
        os.chdir(cwd)
        return True


    def addStorage(self,storage,counter,bootFile):
        arguments = ["objy","AddStorageLocation",
                     "-name","location.{0}".format(counter),
                     "-storageLocation","{0}::{1}".format(storage[1],storage[2]),
                     "noTitle",
                     "-bootfile",bootFile]
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
        p.wait()
        return True

    def createContainers(self,storage,count,bootFile):
        print self.output_string("Container: {0}::{1}".format(storage[1],storage[2]),True,False)
        for i in ["Vertex","Edge","Connector"]:
            arguments = ["objy","CreateContainers",
                         "-count",str(count),
                         "-modelName","InfiniteGraph",
                         "-group",i,
                         "-storageLocation","{0}::{1}".format(storage[1],storage[2]),
                         "-noTitle",
                         "-quiet",
                         "-bootfile",bootFile]
            p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
            p.wait()
            pass
        return True

    def listStorage(self,bootFile):
        arguments = ["objy","ListStorage",
                     "noTitle",
                     "-bootfile",bootFile]
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
        p.wait()
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

    def mkpath(self,address,location,project,counter):
        path = os.path.join(location,"{0}".format(project,counter))
        self.mkdir(path)
        return path
                
    def operate(self):
        if operations.operation.operate(self):
            rootPath = self.getSingleOption("root")
            configParameter = self.getSingleOption("config")
            self.project = self.getSingleOption("project")
            if self.project == None:
                self.error("Project is not given.")
                return False
            rootPath = os.path.abspath(rootPath)
            self.working_path = os.path.join(rootPath,"working")
            project_path = os.path.join(self.working_path,self.project)
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
                print self.output_string("Making path",True,True)
                counter = 0
                bootPath = None
                storage = []
                for host in self.config.hosts:
                    for disk in host.disks:
                        p = self.mkpath(host.address,disk.location,self.project,counter)
                        if bootPath == None:
                            print self.output_string("Bootpath:{0}".format(p),True,True)
                            bootPath = p
                            pass
                        storage.append(("location.{0}".format(counter),host.address,p))
                        print self.output_string("Path:{0}".format(p),True,True)
                        counter += 1
                        pass
                    pass
                propertyFile = ig_property.PropertyFile(os.path.join(project_path,"properties","bootstrap.properties"))
                print self.output_string("Generating bootstrap propertyFile {0}".format(propertyFile.fileName),True,False)
                propertyFile.setLockServer(self.config.lockserver)
                propertyFile.setBootPath(bootPath)
                page_size = self.getSingleOption("page_size")
                if page_size:
                    propertyFile.setPageSize(pow(2,int(page_size)))
                    pass
                propertyFile.generate()
                
                print self.output_string("Creating graph",True,True)
                self.createGraph(self.project,propertyFile,self.hasOption("no_index"))
                locationFile = ig_property.LocationConfigFile(os.path.join(project_path,"properties","location.config"))
                print self.output_string("Generating location preference {0}".format(locationFile.fileName),True,False)
                locationFile.generate(storage)

                counter = 0
                bootFile = os.path.join(bootPath,"{0}.boot".format(self.project))
                for i in storage:
                    self.addStorage(i,counter,bootFile)
                    counter += 1
                    pass
                self.listStorage(bootFile)
                containers = self.getSingleOption("containers")
                if containers > 0:
                    for i in storage:
                        self.createContainers(i,containers,bootFile)
                        pass
                    pass
                return True
            else:
                self.error("Error processing config file.")
                return False
            return False
        return False
