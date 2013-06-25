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
        self.add_argument("ig_version","str",None,"InfiniteGraph version.")
        pass


    def createGraph(self,engine,working_path,project,propertyFile,noIndex):
        cwd = os.getcwd()
        os.chdir(working_path)
        env = self.getEnv(engine.version,engine.home)
        arguments = ["java","-jar",os.path.join(working_path,project,"build","bootstrap.jar"),"-overwrite","-property",propertyFile.fileName]
        if noIndex:
            arguments.append("-no_index")
        print self.output_string("\t"+string.join(arguments),True,False)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        os.chdir(cwd)
        return (p.returncode == 0)


    def addStorage(self,engine,storage,counter,bootFile):
        env = self.getEnv(engine.version,engine.home)
        arguments = ["objy","AddStorageLocation",
                     "-name","location.{0}".format(counter),
                     "-storageLocation","{0}::{1}".format(storage[1],storage[2]),
                     "noTitle",
                     "-bootfile",bootFile]
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        return (p.returncode == 0)

    def createContainers(self,engine,storage,count,bootFile):
        print self.output_string("Container: {0}::{1}".format(storage[1],storage[2]),True,False)
        env = self.getEnv(engine.version,engine.home)
        for i in ["Vertex","Edge","Connector"]:
            arguments = ["objy","CreateContainers",
                         "-count",str(count),
                         "-modelName","InfiniteGraph",
                         "-group",i,
                         "-storageLocation","{0}::{1}".format(storage[1],storage[2]),
                         "-noTitle",
                         "-quiet",
                         "-bootfile",bootFile]
            p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
            p.wait()
            if p.returncode != 0:
                return False
            pass
        return True

    def listStorage(self,engine,bootFile):
        env = self.getEnv(engine.version,engine.home)
        arguments = ["objy","ListStorage",
                     "noTitle",
                     "-bootfile",bootFile]
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
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

    def mkpath(self,address,location,version,project):
        path = os.path.join(location,version)
        if not os.path.exists(path):
            self.mkdir(path)
            pass
        path = os.path.join(path,"{0}".format(project))
        self.mkdir(path)
        return path

  
            
    def operate(self):
        if operations.operation.operate(self):
            rootPath = self.getSingleOption("root")
            configParameter = self.getSingleOption("config")
            ig_version = self.getSingleOption("ig_version")
            project = self.getSingleOption("project")
            if project == None:
                self.error("Project is not given.")
                return False
            if ig_version == None:
                self.error("InfiniteGraph version is not given.")
                return False
            rootPath = os.path.abspath(rootPath)
            working_path = self.setupWorkingPath(rootPath,ig_version)
            project_path = os.path.join(working_path,project)

            if not configParameter:
                self.error("Config Parameter is not given.")
                return False
            configPair = self.getConfigObject(rootPath,configParameter)
            if configPair == None:
                self.error("Unable to get configuration {0}".format(configParameter))
                return False
            (configList,configObject) = configPair
            if configObject == None:
                self.error("Unable to get configuration object {0}".format(configParameter))
                return False
            engine = self.getEngine(configList,ig_version)
            if engine == None:
                self.error("Unable to find configuration for InfiniteGraph version '{0}' using config {1}.".format(ig_version,configParameter))
                return False

            if not os.path.exists(project_path):
                self.warn("Project path '{0}' does not exist. Trying to build project.".format(project))
                build = build_operation.operation()
                build.parse(["--root","{0}".format(rootPath)])
                build.parse(["--ig_home","{0}".format(engine.home)])
                build.parse(["--ig_version","{0}".format(engine.version)])
                build.operate()
                if not os.path.exists(project_path):
                    self.error("Project path '{0}' does not exist after an attempted build.".format(project))
                    return False
                pass
            
            print self.output_string("Bootstrap using\n{0}".format(configObject),True,True)
            print self.output_string("Making path",True,True)
            counter = 0
            bootPath = None
            storage = []
            for host in configObject.hosts:
                for disk in host.disks:
                    p = self.mkpath(host.address,disk.location,ig_version,project)
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
            propertyFile.setLockServer(configObject.lockserver)
            propertyFile.setBootPath(bootPath)
            page_size = self.getSingleOption("page_size")
            if page_size:
                propertyFile.setPageSize(pow(2,int(page_size)))
                pass
            propertyFile.generate()
            
            print self.output_string("Creating graph",True,True)
            self.createGraph(engine,working_path,project,propertyFile,self.hasOption("no_index"))

            locationFile = ig_property.LocationConfigFile(os.path.join(project_path,"properties","location.config"))
            print self.output_string("Generating location preference {0}".format(locationFile.fileName),True,False)
            locationFile.generate(storage)
            
            counter = 0
            bootFile = os.path.join(bootPath,"{0}.boot".format(project))
            for i in storage:
                if not self.addStorage(engine,i,counter,bootFile):
                    self.error("Unable to add storage location.")
                    return False
                counter += 1
                pass
            self.listStorage(engine,bootFile)
            containers = self.getSingleOption("containers")
            if containers > 0:
                for i in storage:
                    if not self.createContainers(engine,i,containers,bootFile):
                        self.error("Unable to create containers.")
                        return False
                    pass
                pass
            return True
        self.error("Unknown error during bootstrap.")
        return False
    pass
