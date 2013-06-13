import os
import math


import sys
import types
import time
import platform
import random
import string
import subprocess
from collections import deque

import ig_property
#import config
import runnable
import db_objects

class operation(runnable.operation):
    def __init__(self):
        runnable.operation.__init__(self)
        self.add_argument("engine","str","ig","engine (ig)")
        self.add_argument("page_size","eval",14,"page size page_size=pow(2,value) valid_values=[10,11,12,13,14,15,16]")
        self.add_argument("diskmap",int,None,"disk map variable (for config).")
        self.add_argument("hostmap","str",None,"host map variable (for config) [remote | local].")
        self.config     = config.ig.Config
        self.rootResultsPath = os.path.abspath("results")
        self.version = None
        self.engine  = "ig"
        self.propertyFile = ig_property.PropertyFile("ig.properties")
        self.db = db_objects.db()
        self.db.create_database()
        self.diskmap = None
        self.hostmap = None
        pass


        
    
    def initialize(self):
        if not os.path.exists(self.config.ig.IG2_Jar):
            print >> sys.stderr,"ERROR: Cannot find jar file: ", self.config.ig.IG2_Jar
            return False
        if not os.path.exists(self.config.ig.IG3_Jar):
            print >> sys.stderr,"ERROR: Cannot find jar file: ", self.config.ig.IG3_Jar
            return False
        if not os.path.exists(self.rootResultsPath):
            self.mkdir(self.rootResultsPath)
            pass
        return True

    def createResultsPath(self,name):
        path = os.path.join(self.rootResultsPath,name)
        if not os.path.exists(path):
            self.mkdir(path)
        return path

    def cleanResultsPath(self,name):
        listing = os.listdir(name)
        for i in listing:
            if i.endswith(".profile"):
                os.remove(os.path.join(name,i))
                pass
            pass
        pass

    def run_ig_command(self,config,version,binary,arguments):
        _command = None
        env  = os.environ.copy()
        lib_path_list = [os.path.join(self.config.Root[version],"lib")]
        lib  = string.join(lib_path_list,":")
        env["DYLD_LIBRARY_PATH"] = lib
        env["LD_LIBRARY_PATH"] = lib
        path_list = env["PATH"]
        env["PATH"] = os.path.join(self.config.Root[version],"bin") + ":" + path_list
        _arguments = [binary]
        _arguments += arguments
        #print _arguments
        
        p = subprocess.Popen(_arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        return  p.returncode
        

    def makedirs(self,path):
        if not os.path.exists(path):
            os.makedirs(path)
            pass
        pass

    def initialize_property(self,version):
        self.propertyFile._initialize_()
        if self.hostmap != None:
            disks = self.config.Disks[version]
            if self.diskmap:
                dmap = self.config.GetDiskMap()
                disks = dmap[self.diskmap][version]
                pass
            hostname = self.config.GetHostMap()[self.hostmap][version]
            self.propertyFile.properties["IG.LockServerHost"] = hostname
            self.propertyFile.properties["IG.MasterDatabaseHost"] = hostname
            self.propertyFile.properties["IG.MasterDatabasePath"] = "%s"%(disks[0].device)
            pass
        pass
            
    def get_boot_file_path(self,version):
        if self.hostmap == None:
            return self.config.BootFilePath[version]
        hostmap = self.config.GetHostMap()
        hostname = self.config.GetHostMap()[self.hostmap][version]
        print "using host name:",hostname
        return "%s::%s"%(hostname,self.config.BootFilePath[version])

    def ig_setup_placement(self,version,propertyObject):
        counter = 1
        if version == "ig2":
            propertyObject.properties["IG.Placement.ImplClass"]="com.infinitegraph.impl.plugins.adp.DistributedPlacement"
            pass
        disks = self.config.Disks[version]
        if self.diskmap:
            print "Using disk map:",self.diskmap
            dmap = self.config.GetDiskMap()
            if dmap.has_key(self.diskmap):
                disks = dmap[self.diskmap][version]
                pass
            else:
                print "Unable to find disk map key:",self.diskmap
                pass
            pass
        if self.hostmap == None:
            self.makedirs(self.config.BootFilePath[version])
            pass
        for disk in disks:
            if self.hostmap == None:
                self.makedirs(disk.device)
                pass
            if version == "ig2":
                hostname = disk.host
                if self.hostmap != None:
                    hostmap = self.config.GetHostMap()
                    hostname = self.config.GetHostMap()[self.hostmap][version]
                    pass
                storageName  = "storage_%d"%(counter)
                propertyObject.properties["IG.Placement.Distributed.Location.%s"%(disk.name)]  = "%s::%s"%(hostname,disk.device)
                propertyObject.properties["IG.Placement.Distributed.StorageSpec.%s.ContainerRange"%(storageName)]="1:*"
                propertyObject.properties["IG.Placement.Distributed.GroupStorage.GraphData"]="%s:%s"%(disk.name,storageName)
                counter += 1
                pass
            pass
        pass        


    def ig_setup_Location(self,version,propertyObject):
        if version == "ig3":
            locationConfig = ig_property.LocationConfigFile(os.path.join(self.config.BootFilePath[version],"Location.config"))
            locationConfig.generate(self.config.Disks[version])
            propertyObject.properties["IG.Placement.PreferenceRankFile"] = os.path.join(self.config.BootFilePath[version],"Location.config")
            counter = 1
            disks = self.config.Disks[version]
            if self.diskmap:
                print "Using disk map:",self.diskmap
                dmap = self.config.GetDiskMap()
                if dmap.has_key(self.diskmap):
                    disks = dmap[self.diskmap][version]
                    pass
                else:
                    print "Unable to find disk map key:",self.diskmap
                    pass
                pass
            for disk in disks:
                hostname = disk.host
                if self.hostmap != None:
                    hostmap = self.config.GetHostMap()
                    hostname = self.config.GetHostMap()[self.hostmap][version]
                    pass
                arguments = ["AddStorageLocation",
                             "-noTitle",
                             "-name",disk.name,
                             "-storageLocation","%s::%s"%(hostname,disk.device),
                             "-bootfile",os.path.join(self.get_boot_file_path(version),"ig3.boot")
                             #os.path.join(self.config.BootFilePath[version],"ig3.boot")
                             ]
                
                if self.verbose == 0:
                    arguments.append("-quiet")
                    pass
                self.run_ig_command(self.config,version,"objy",arguments)
                counter += 1
            pass
        pass
    
    def ig_run(self,version,propertyObject,operation,index):
        env  = os.environ.copy()
        engine = None
        jarFile = None
        propertyObject.properties["IG.BootFilePath"] = self.get_boot_file_path(version) 
        propertyObject.fileName = os.path.join(os.getcwd(),"%s.properties"%(version))
        propertyObject.generate()
        lib_path_list = [os.path.join(self.config.Root[version],"lib")]
        lib = string.join(lib_path_list,":")
        env["DYLD_LIBRARY_PATH"] = lib
        env["LD_LIBRARY_PATH"] = lib
        path_list = env["PATH"]
        env["PATH"] = os.path.join(self.config.Root[version],"bin") + ":" + path_list
        engine = version
        jarFile = self.config.BenchmarkJar[version]
        binary = "java"
        arguments = [binary,"-jar",jarFile,"-engine",engine,"-operation",operation,"-property",propertyObject.fileName,"-index",index,"-verbose",str(0)]
        arguments.append("-db")
        arguments.append(engine)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        
        return  p.returncode


    def ig_v_search(self,search_file_name,version,propertyObject,index,size,graph_size,threads,txsize,profileName):
        env  = os.environ.copy()
        engine = None
        jarFile = None
        propertyObject.properties["IG.BootFilePath"] = self.get_boot_file_path(version)
        propertyObject.fileName = os.path.join(os.getcwd(),"%s.properties"%(version))
        propertyObject.generate()
        lib_path_list = [os.path.join(self.config.Root[version],"lib")]
        lib = string.join(lib_path_list,":")
        env["DYLD_LIBRARY_PATH"] = lib
        env["LD_LIBRARY_PATH"] = lib
        path_list = env["PATH"]
        env["PATH"] = os.path.join(self.config.Root[version],"bin") + ":" + path_list
        engine = version
        jarFile = self.config.BenchmarkJar[version]
        binary = "java"
        arguments = [binary,"-jar",jarFile,"-engine",engine,"-operation","search","-property",propertyObject.fileName,"-index",index,"-verbose",str(10),
                     "-searchlist",search_file_name,
                     "-size",str(graph_size),
                     "-t",str(threads),
                     "-tsize",str(txsize),
                     "-profile",profileName,
                     "-db",engine
                     ]
     
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        return  p.returncode
    
    def ig_v_ingest(self,version,propertyObject,index,size,block,threads,txsize,profileName,map_oid):
        env  = os.environ.copy()
        engine = None
        jarFile = None
        propertyObject.properties["IG.BootFilePath"] = self.get_boot_file_path(version)
        propertyObject.fileName = os.path.join(os.getcwd(),"%s.properties"%(version))
        propertyObject.generate()
        lib_path_list = [os.path.join(self.config.Root[version],"lib")]
        lib = string.join(lib_path_list,":")
        env["DYLD_LIBRARY_PATH"] = lib
        env["LD_LIBRARY_PATH"] = lib
        path_list = env["PATH"]
        env["PATH"] = os.path.join(self.config.Root[version],"bin") + ":" + path_list
        engine = version
        jarFile = self.config.BenchmarkJar[version]
        binary = "java"
        arguments = [binary,"-jar",jarFile,"-engine",engine,"-operation","standard_ingest","-property",propertyObject.fileName,"-index",index,"-verbose",str(0),
                     "-size",str(size),
                     "-block",str(block),
                     "-vit",str(threads),
                     "-tsize",str(txsize),
                     "-profile",profileName,
                     "-db",engine
                     ]
        if map_oid:
            arguments.append("-uselocalmap")
            pass
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        return  p.returncode


    def ig_e_pipeline_ingest(self,version,propertyObject,scale,threads,txsize,profileName,elist_name):
        env  = os.environ.copy()
        engine = None
        jarFile = None
        propertyObject.properties["IG.BootFilePath"] = self.get_boot_file_path(version)
        propertyObject.fileName = os.path.join(os.getcwd(),"%s.properties"%(version))
        propertyObject.generate()
        lib_path_list = [os.path.join(self.config.Root[version],"lib")]
        lib = string.join(lib_path_list,":")
        env["DYLD_LIBRARY_PATH"] = lib
        env["LD_LIBRARY_PATH"] = lib
        path_list = env["PATH"]
        env["PATH"] = os.path.join(self.config.Root[version],"bin") + ":" + path_list
        engine = version
        jarFile = self.config.BenchmarkJar[version]
        binary = "java"
        arguments = [binary,"-jar",jarFile,"-engine",engine,"-operation","accelerated_e_ingest","-property",propertyObject.fileName,"-verbose",str(4),
                     "-scale",str(scale),
                     "-eit",str(threads),
                     "-tsize",str(txsize),
                     "-profile",profileName,
                     "-db",engine,
                     "-uselocalmap",
                     "-edgelist",elist_name
                     ]
        #print string.join(arguments)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        return  p.returncode

    def ig_e_standard_ingest(self,version,propertyObject,index,scale,threads,txsize,profileName,elist_name):
        env  = os.environ.copy()
        engine = None
        jarFile = None
        propertyObject.properties["IG.BootFilePath"] = self.get_boot_file_path(version)
        propertyObject.fileName = os.path.join(os.getcwd(),"%s.properties"%(version))
        propertyObject.generate()
        lib_path_list = [os.path.join(self.config.Root[version],"lib")]
        lib = string.join(lib_path_list,":")
        env["DYLD_LIBRARY_PATH"] = lib
        env["LD_LIBRARY_PATH"] = lib
        path_list = env["PATH"]
        env["PATH"] = os.path.join(self.config.Root[version],"bin") + ":" + path_list
        engine = version
        jarFile = self.config.BenchmarkJar[version]
        binary = "java"
        arguments = [binary,"-jar",jarFile,"-engine",engine,"-operation","standard_e_ingest","-index",index,
                     "-property",propertyObject.fileName,
                     "-verbose",str(40),
                     "-scale",str(scale),
                     "-eit",str(threads),
                     "-tsize",str(txsize),
                     "-profile",profileName,
                     "-db",engine,
                     "-uselocalmap",
                     "-edgelist",elist_name
                     ]
        #print string.join(arguments)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        return  p.returncode

    def ig_navigate(self,ttype,version,propertyObject,index,scale,threads,txsize,profileName,elist_name):
        env  = os.environ.copy()
        engine = None
        jarFile = None
        propertyObject.properties["IG.BootFilePath"] = self.get_boot_file_path(version)
        propertyObject.fileName = os.path.join(os.getcwd(),"%s.properties"%(version))
        propertyObject.generate()
        lib_path_list = [os.path.join(self.config.Root[version],"lib")]
        lib = string.join(lib_path_list,":")
        env["DYLD_LIBRARY_PATH"] = lib
        env["LD_LIBRARY_PATH"] = lib
        path_list = env["PATH"]
        env["PATH"] = os.path.join(self.config.Root[version],"bin") + ":" + path_list
        engine = version
        jarFile = self.config.BenchmarkJar[version]
        binary = "java"
        arguments = [binary,"-jar",jarFile,"-engine",engine,"-operation",ttype,"-index",index,
                     "-property",propertyObject.fileName,
                     "-verbose",str(40),
                     "-size",str(scale),
                     "-eit",str(threads),
                     "-tsize",str(txsize),
                     "-profile",profileName,
                     "-db",engine,
                     "-uselocalmap",
                     "-edgelist",elist_name
                     ]
        #print string.join(arguments)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        return  p.returncode

    Known_Engines = {"ig2":"InfiniteGraph v2.1","ig3":"InfiniteGraph v3.0"}
    
    def is_known_engine(self,name):
        name = name.lower()
        return self.Known_Engines.has_key(name)
    
    def get_engine_object(self,name):
        name = name.lower()
        engine = None
        if self.is_known_engine(name):
            engine = self.db.create_unique_object(db_objects.model.engine,"name",name,description=self.Known_Engines[name])
            pass
        return engine

    def setup_engine_objects(self):
        self.engine_objects = []
        for i in self.engine:
            e = self.get_engine_object(i)
            if e != None:
                self.engine_objects.append(e)
            else:
                self.error("Unknown database engine '%s'."%(i))
                self.error("Known engines are %s."%(self.Known_Engines.keys()))
                return False
            pass
        return True

    def setup_page_sizes(self,_page_size):
        self.page_size = []
        for i in _page_size:
            if i < 10:
                self.warn("Invalid page_size (%d) given must be between [10,16], setting to 10"%(i))
                i = 10
            elif i > 16:
                self.warn("Invalid page_size (%d) given must be between [10,16], setting to 16"%(i))
                i = 16
                pass
            self.page_size.append(pow(2,i))
            pass
        pass
    
    def operate(self):
        if runnable.operation.operate(self):
            self.engine = self.getOption("engine")
            self.diskmap = self.getSingleOption("diskmap")
            self.hostmap = self.getSingleOption("hostmap")
            _page_size = self.getOption("page_size")
            self.setup_page_sizes(_page_size)
            return self.setup_engine_objects()
        return False
