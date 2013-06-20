import operations
import os
import time
import sys
import bootstrap_operation
import ig_property
import config
import dataset_operation
import string
import subprocess
import db
import db_model


class operation(operations.operation):
    def __init__(self):
        operations.operation.__init__(self,"vertex_ingest")
        self.add_argument("root","str",".","Root path.")
        self.add_argument("template","str",None,"template name")
        self.add_argument("page_size","int","14","page size to use applicable when a new graph is created (use pows of two) (12 for 4K up to 16 for 64K)")
        self.add_argument("config","str",None,"config name")
        self.add_argument("use_index","int",1,"use index")
        self.add_argument("new","int",1,"create new graph")
        self.add_argument("size","eval",100000,"number of vertices.")
        self.add_argument("threads","eval",1,"number of threads")
        self.add_argument("txsize","eval",10000,"transaction size")
        self.add_argument("txlimit","int",-1,"limit the number of transactions to a given number")
        self.add_argument("cache","eval",(1000,500000),"cache size given as a set of tuplets (in kB) (init,max) or [(init_1,max_1),(init_2,max_2),.....]")
        self.add_argument("ig_version","str",None,"InfiniteGraph version to use.")
        self.tag_object = None
        self.case_object = None
        self.cache = None
        pass

    def is_runnable(self):
        return True

    def run(self,db,suite,case,data,**kwargs):
        self.case_object = case
        self.verbose = kwargs["verbose"]
        self.db = db

        template     = self.getOption_data(data,"template")
        configNames  = self.getOption_data(data,"config")
        page_size    = self.getOption_data(data,"page_size")
        cache        = self.getOption_data(data,"cache")
        useIndex     = self.getOption_data(data,"use_index")
        new_graph    = self.getOption_data(data,"new")
        size         = self.getOption_data(data,"size")
        threads      = self.getOption_data(data,"threads")
        txsize       = self.getOption_data(data,"txsize")
        txlimit      = self.getOption_data(data,"txlimit")
        cache        = self.getOption_data(data,"cache")
        ig_version   = self.getOption_data(data,"ig_version")
        self.tag_object = self.db.create_unique_object(db_model.tag,"name",kwargs["tag"],
                                                       timestamp=self.db.now_string(True))
        
        rootPath = os.path.dirname(db_model.suite.RootSuite.get_path())
        self.run_operation(ig_version,rootPath,template,configNames,page_size,cache,useIndex,new_graph,size,threads,txsize,txlimit)
        pass

    def removeProfileData(self,working_path):
        events  = os.path.join(working_path,"benchmark.events")
        profile = os.path.join(working_path,"benchmark.profile")
        if os.path.exists(events):
            os.remove(events)
            pass
        if os.path.exists(profile):
            os.remove(profile)
            pass
        pass

    def readProfileData(self,working_path):
        eventsName  = os.path.join(working_path,"benchmark.events")
        profileName = os.path.join(working_path,"benchmark.profile")
        events  = []
        profile = None
        if os.path.exists(eventsName):
            f = file(eventsName,"r")
            line = f.readline()
            while len(line):
                line = eval(line)
                events.append(line)
                line = f.readline()
                pass
            pass
        if os.path.exists(profileName):
            f = file(profileName,"r")
            line = f.readline()
            while len(line):
                line = eval(line)
                profile = line
                line = f.readline()
                pass
            pass
        return (events,profile)
        
    def __run__(self,engine,working_path,jar,dataset,propertyName,threads,txSize,txLimit):
        cwd = os.getcwd()
        os.chdir(working_path)
        self.removeProfileData(working_path)
        env = self.getEnv(engine.version,engine.home)
        arguments = ["java","-jar",jar,
                     "-op_path",dataset,
                     "-property",propertyName,
                     "-threads",str(threads),
                     "-tx_size",str(txSize),
                     "-tx_type","write",
                     "-tx_limit",str(txLimit),
                     "-ops","V",
                     "-no_map"
                     ]
        print string.join(arguments)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        os.chdir(cwd)
        if p.returncode != 0:
            return None
        (events,profile) = self.readProfileData(working_path)
        #self.removeProfileData(working_path)
        return (events,profile) 

    
    def run_operation(self,ig_version,rootPath,template,configNames,page_size,cache,useIndex,new_graph,size,threads,txsize,txlimit):
        if template == None:
            self.error("template is required but not given.")
            return False
        if configNames == None:
            self.error("config name is required but not given.")
            return False
        if ig_version == None:
            self.error("InfiniteGraph version is not given.")
            return False
        rootPath = os.path.abspath(rootPath)
        for iVersion in ig_version:
            for iTemplate in template:
                for iSize in size:
                    working_path = self.setupWorkingPath(rootPath,iVersion)
                    generateDatasetPath = self.GenerateDataset(rootPath,iTemplate,iSize,True)
                    for iConfig in configNames:
                        for iPageSize in page_size:
                            for iCache in cache:
                                for iUseIndex in useIndex:
                                    for iThreads in threads:
                                        for iTxSize in txsize:
                                            for iTxLimit in txlimit:
                                                if new_graph:
                                                    print "Create new graph"
                                                    print "template {0} config {1} page_size {2} cache {3} useIndex {4} size {5} threads {6} txsize {7}".format(iTemplate,iConfig,iPageSize,iCache,iUseIndex,iSize,iThreads,iTxSize) 
                                                    bootstrap = bootstrap_operation.operation()
                                                    bootstrap.parse([
                                                        "--root","{0}".format(rootPath),
                                                        "--config",iConfig,
                                                        "--project",iTemplate,
                                                        "--page_size",iPageSize,
                                                        "--no_index",(not iUseIndex),
                                                        "--containers",1,
                                                        "--ig_version",iVersion
                                                        ])
                                                    if not bootstrap.operate():
                                                        self.error("--Failed to bootstrap database.")
                                                        return False
                                                    pass
                                                configPair = self.getConfigObject(rootPath,iConfig)
                                                if configPair == None:
                                                    self.error("Unable to get configuration {0}".format(iConfig))
                                                    return False
                                                (configList,configObject) = configPair
                                                if not configObject:
                                                    self.error("Unable to get Config object")
                                                    return False
                                                engine = self.getEngine(configList,iVersion)
                                                if engine == None:
                                                    self.error("Unable to find configuration for InfiniteGraph version '{0}' using config {1}.".format(ig_version,iConfig))
                                                    return False
                                                engine_object = self.db.create_unique_object(db_model.engine,"name",engine.version,description=engine.version)
                                                config_object = self.db.create_unique_object(db_model.config,"name",configObject.getFullName(),description=configObject.getFullName())
                                                
                                                project_path = os.path.join(working_path,iTemplate)
                                                bootPath = None
                                                if len(configObject.hosts) > 0:
                                                    if len(configObject.hosts[0].disks) > 0:
                                                        p = os.path.join(configObject.hosts[0].disks[0].location,iVersion,iTemplate)
                                                        print self.output_string("Bootpath:{0}".format(p),True,True)
                                                        bootPath = p
                                                        pass
                                                    pass
                                                propertyFile = ig_property.PropertyFile(os.path.join(project_path,"properties","vertex_ingest.properties"))
                                                print self.output_string("Generating ingest propertyFile {0}".format(propertyFile.fileName),True,False)
                                                propertyFile.setLockServer(configObject.lockserver)
                                                propertyFile.setBootPath(bootPath)
                                                propertyFile.generate()
                                                propertyFile.setPageSize(pow(2,iPageSize))
                                                jar = os.path.join(working_path,iTemplate,"build","benchmark.jar")
                                                (events,profile) = self.__run__(engine,working_path,jar,generateDatasetPath,propertyFile.fileName,iThreads,iTxSize,iTxLimit)
                                                if self.case_object:
                                                    platform_object = self.db.create_unique_object(db_model.platform,"name",self.db.hostname(),type=profile["os"])
                                                    if iUseIndex:
                                                        index_object = self.db.create_unique_object(db_model.index_type,"name","gr")
                                                    else:
                                                        index_object = self.db.create_unique_object(db_model.index_type,"name","none")
                                                        pass
                                                    profile_data = profile["data"]
                                                    case_data_object = self.db.create_object(db_model.case_data,
                                                                                             timestamp=self.db.now_string(True),
                                                                                             case_id=self.case_object.id,
                                                                                             engine_id=engine_object.id,
                                                                                             tag_id=self.tag_object.id,
                                                                                             size=profile_data["size"],
                                                                                             time=profile["time"],
                                                                                             memory_init=profile["memInit"],
                                                                                             memory_used=profile["memUsed"],
                                                                                             memory_committed=profile["memCommitted"],
                                                                                             memory_max=profile["memMax"],
                                                                                             rate=profile_data["rate"],
                                                                                             page_size=iPageSize,
                                                                                             cache_init=propertyFile.getInitCache(),
                                                                                             cache_max=propertyFile.getMaxCache(),
                                                                                             tx_size=iTxSize,
                                                                                             platform_id=platform_object.id,
                                                                                             threads=iThreads,
                                                                                             index_id=index_object.id,
                                                                                             config_id=config_object.id,
                                                                                             status=1
                                                                                             )
                                                    case_data_key = case_data_object.generateKey()
                                                    case_data_stat_object = self.db.fetch_using_generic(db_model.case_data_stat,
                                                                                                        key=case_data_key,
                                                                                                        case_id=self.case_object.id
                                                                                                        )
                                                    if (len(case_data_stat_object) == 0):
                                                        case_data_stat_object = self.db.create_unique_object(db_model.case_data_stat,
                                                                                                             "key",case_data_key,
                                                                                                             case_id=self.case_object.id
                                                                                                             )
                                                    else:
                                                        case_data_stat_object = case_data_stat_object[0]
                                                        pass
                                                    case_data_stat_object.addCounter()
                                                    case_data_stat_object.setRateStat(profile_data["rate"])
                                                    case_data_stat_object.setTimeStat(profile["time"])
                                                    case_data_stat_object.setMemInitStat(profile["memInit"])
                                                    case_data_stat_object.setMemUsedStat(profile["memUsed"])
                                                    case_data_stat_object.setMemCommittedStat(profile["memCommitted"])
                                                    case_data_stat_object.setMemMaxStat(profile["memMax"])
                                                    self.db.update(case_data_stat_object)
                                                    pass
                                                pass
                                            pass
                                        pass
                                    pass
                                pass
                            pass
                        pass
                    pass
                pass
            pass
        else:
            pass
        return True

    
    def operate(self):
        if operations.operation.operate(self):
            rootPath = self.getSingleOption("root")
            template = self.getOption("template")
            configNames   = self.getOption("config")
            page_size     = self.getOption("page_size")
            cache         = self.getOption("cache")
            useIndex      = self.getOption("use_index")
            new_graph     = self.getSingleOption("new")
            size          = self.getOption("size")
            threads       = self.getOption("threads")
            txsize        = self.getOption("txsize")
            txlimit       = self.getOption("txlimit")
            cache         = self.getOption("cache")
            ig_version    = self.getOption("ig_version")
            self.run_operation(ig_version,rootPath,template,configNames,page_size,cache,useIndex,new_graph,size,threads,txsize,txlimit)
        return False

        
