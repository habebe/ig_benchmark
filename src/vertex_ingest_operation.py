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
        self.add_argument("cache","eval",(1000,500000),"cache size given as a set of tuplets (in kB) (init,max) or [(init_1,max_1),(init_2,max_2),.....]")
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
        cache        = self.getOption_data(data,"cache")
                
        #page_size = self.setup_page_sizes(page_size)
        self.tag_object = self.db.create_unique_object(db_model.tag,"name",kwargs["tag"],
                                                       timestamp=self.db.now_string(True))
        
        rootPath = os.path.dirname(db_model.suite.RootSuite.path)
        self.run_operation(rootPath,template,configNames,page_size,cache,useIndex,new_graph,size,threads,txsize)
        pass

    def __run_operation(self):
        for engine in self.engine_objects:
            for _index in self.index: 
                for _page_size in self.page_size:
                    for _v_size in self.graph_size:
                        for _threads in self.threads:
                            for _txsize in self.txsize:
                                for _cache in self.cache:
                                    print 
                                    self.initialize_property(engine.name)
                                    self.propertyFile.setInitCache(_cache[0])
                                    self.propertyFile.setMaxCache(_cache[1])
                                    self.propertyFile.properties["IG.PageSize"] = _page_size
                                    if self.new_graph:
                                        self.ig_setup_placement(engine.name,self.propertyFile)
                                        _s_ = "\tgraph(%s) create index:%s page_size:%d"%(engine.name,_index,_page_size)
                                        print self.output_string(_s_,base.Colors.Blue,True),
                                        sys.stdout.flush()
                                        start = time.clock()
                                        self.ig_run(engine.name,self.propertyFile,"create",_index)
                                        elapsed = (time.clock() - start)
                                        print self.output_string(str(elapsed),base.Colors.Red,False)
                                        self.ig_setup_Location(engine.name,self.propertyFile)
                                        pass
                                    _s_ = "\tgraph(%s) ingest vertices index:%s page_size:%d tx_size:%d size:%d diskmap:%s"%(engine.name,_index,_page_size,_txsize,_v_size,str(self.diskmap))
                                    print self.output_string(_s_,base.Colors.Blue,True)
                                    profileName = "test.profile"
                                    if self.tag_object:
                                        profileName = self.tag_object.name +"_"+ self.db.now_string(True)  +"_"+ ".profile"
                                        profileName = profileName.replace(" ","_")
                                        pass
                                    self.ig_v_ingest(engine.name,self.propertyFile,_index,_v_size,0,_threads,_txsize,profileName,self.map_oid)
                                    if self.case_object:
                                        f = file(profileName,"r")
                                        line = f.readline()
                                        data = eval(line)
                                        platform_object = self.db.create_unique_object(db_objects.model.platform,"name",data["os"])
                                        index_object = self.db.create_unique_object(db_objects.model.index_type,"name",_index)
                                    
                                        case_data_object = self.db.create_object(db_objects.model.case_data,
                                                                                 timestamp=self.db.now_string(True),
                                                                                 case_id=self.case_object.id,
                                                                                 tag_id=self.tag_object.id,
                                                                                 engine_id=engine.id,
                                                                                 size=data["size"],
                                                                                 time=data["time"],
                                                                                 memory_init=data["mem_init"],
                                                                                 memory_used=data["mem_used"],
                                                                                 memory_committed=data["mem_committed"],
                                                                                 memory_max=data["mem_max"],
                                                                                 op_size=data["opsize"],
                                                                                 rate=data["rate"],
                                                                                 page_size=_page_size,
                                                                                 cache_init=self.propertyFile.getInitCache(),
                                                                                 cache_max=self.propertyFile.getMaxCache(),
                                                                                 tx_size=_txsize,
                                                                                 platform_id=platform_object.id,
                                                                                 threads=_threads,
                                                                                 index_id=index_object.id,
                                                                                 status=1
                                                                                 )
                                        if self.diskmap:
                                            case_data_object.setDataValue("diskmap",self.diskmap)
                                            self.db.update(case_data_object)
                                            pass
                                        case_data_key = case_data_object.generateKey()
                                        case_data_stat_object = self.db.fetch_using_generic(db_objects.model.case_data_stat,
                                                                                            key=case_data_key,
                                                                                            case_id=self.case_object.id
                                                                                            )
                                        if (len(case_data_stat_object) == 0):
                                            case_data_stat_object = self.db.create_unique_object(db_objects.model.case_data_stat,
                                                                                                 "key",case_data_key,
                                                                                                 case_id=self.case_object.id
                                                                                                 )
                                        else:
                                            case_data_stat_object = case_data_stat_object[0]
                                            pass
                                        case_data_stat_object.addCounter()
                                        case_data_stat_object.setRateStat(data["rate"])
                                        case_data_stat_object.setTimeStat(data["time"])
                                        case_data_stat_object.setMemInitStat(data["mem_init"])
                                        case_data_stat_object.setMemUsedStat(data["mem_used"])
                                        case_data_stat_object.setMemCommittedStat(data["mem_committed"])
                                        case_data_stat_object.setMemMaxStat(data["mem_max"])
                                        self.db.update(case_data_stat_object)
                                        f.close()
                                        os.remove(profileName)
                                        pass
                                    pass
                                pass
                            pass
                        pass
                    pass
                pass
            pass
        pass

    def getConfigList(self,rootPath,name):
        configParameter = name.split(":")
        configFileName = configParameter[0]
        if not configFileName.endswith(".xml"):
            configFileName = configFileName + ".xml"
            pass
        configName = None
        if len(configParameter) == 2:
            configName = configParameter[1]
            pass
        
        configFileName = os.path.join(rootPath,"config",configFileName)
        configList = config.parse(configFileName,self)
        try:
            configList = config.parse(configFileName,self)
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            self.error("Error while parsing config file. {0}".format(exc_obj))
            return None
        configObject = None
        if configList and len(configList.configs) > 0:
            if configName:
                for i in configList.configs:
                    if i.name == configName:
                        configObject = i
                        pass
                    pass
                if configObject == None:
                    self.error("Unable to find config with a name {0}.".format(self.configName))
                    return None
                pass
            else:
                configObject = configList.configs[0]
                pass
            pass
        return configObject


    def removeProfileData(self,working_path):
        events  = os.path.join(working_path,"write.events")
        profile = os.path.join(working_path,"write.profile")
        if os.path.exists(events):
            os.remove(events)
            pass
        if os.path.exists(profile):
            os.remove(profile)
            pass
        pass

    def readProfileData(self,working_path):
        eventsName  = os.path.join(working_path,"write.events")
        profileName = os.path.join(working_path,"write.profile")
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
        
    def __run__(self,working_path,jar,dataset,propertyName,threads,txSize):
        cwd = os.getcwd()
        os.chdir(working_path)
        self.removeProfileData(working_path)
        arguments = ["java","-jar",jar,
                     "-dataset",dataset,
                     "-property",propertyName,
                     "-threads",str(threads),
                     "-tx_size",str(txSize),
                     "-ops","V"
                     ]
        print string.join(arguments)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
        p.wait()
        os.chdir(cwd)
        (events,profile) = self.readProfileData(working_path)
        #self.removeProfileData(working_path)
        return (events,profile) 

    
    def run_operation(self,rootPath,template,configNames,page_size,cache,useIndex,new_graph,size,threads,txsize):
        if template == None:
            self.error("template is required but not given.")
            return False
        if configNames == None:
            self.error("config name is required but not given.")
            return False
        rootPath = os.path.abspath(rootPath)
        if new_graph:
            for iTemplate in template:
                for iSize in size:
                    working_path = os.path.join(rootPath,"working")
                    print "Generating template {0} dataset {1} size {2}".format(iTemplate,working_path,iSize)
                    dataset = dataset_operation.operation()
                    dataset.parse([
                        "--root","{0}".format(rootPath),
                        "--template",iTemplate,
                        "--size",iSize,
                        "--source",working_path,
                        "--target",working_path,
                        "--vertex_only"
                        ])
                    dataset.operate()
                    for iConfig in configNames:
                        for iPageSize in page_size:
                            for iCache in cache:
                                for iUseIndex in useIndex:
                                    for iThreads in threads:
                                        for iTxSize in txsize:
                                            print "Create new graph"
                                            print "template {0} config {1} page_size {2} cache {3} useIndex {4} size {5} threads {6} txsize {7}".format(iTemplate,iConfig,iPageSize,iCache,iUseIndex,iSize,iThreads,iTxSize) 
                                            bootstrap = bootstrap_operation.operation()
                                            bootstrap.parse([
                                                "--root","{0}".format(rootPath),
                                                "--config",iConfig,
                                                "--project",iTemplate,
                                                "--page_size",iPageSize,
                                                "--no_index",(not iUseIndex),
                                                "--containers",1
                                                ])
                                            bootstrap.operate()
                                            configObject = self.getConfigList(rootPath,iConfig)
                                            if not configObject:
                                                self.error("Unable to get Config object")
                                                return False
                                            project_path = os.path.join(working_path,iTemplate)
                                            bootPath = None
                                            if len(configObject.hosts) > 0:
                                                if len(configObject.hosts[0].disks) > 0:
                                                    p = os.path.join(configObject.hosts[0].disks[0].location,iTemplate)
                                                    print self.output_string("Bootpath:{0}".format(p),True,True)
                                                    bootPath = p
                                                    pass
                                                pass
                                            propertyFile = ig_property.PropertyFile(os.path.join(project_path,"properties","vertex_ingest.properties"))
                                            print self.output_string("Generating bootstrap propertyFile {0}".format(propertyFile.fileName),True,False)
                                            propertyFile.setLockServer(configObject.lockserver)
                                            propertyFile.setBootPath(bootPath)
                                            propertyFile.generate()
                                            propertyFile.setPageSize(pow(2,iPageSize))
                                            jar = os.path.join(working_path,iTemplate,"build","write.jar")
                                            (events,profile) = self.__run__(working_path,jar,working_path,propertyFile.fileName,iThreads,iTxSize)
                                            if self.case_object:
                                                platform_object = self.db.create_unique_object(db_model.platform,"name",profile["os"])
                                                if iUseIndex:
                                                    index_object = self.db.create_unique_object(db_model.index_type,"name","gr")
                                                else:
                                                    index_object = self.db.create_unique_object(db_model.index_type,"name","none")
                                                    pass
                                                profile_data = profile["data"]
                                                case_data_object = self.db.create_object(db_model.case_data,
                                                                                         timestamp=self.db.now_string(True),
                                                                                         case_id=self.case_object.id,
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
        else:
            pass
        return True

    
    def operate(self):
        if operations.operation.operate(self):
            rootPath = self.getSingleOption("root")
            template = self.getOption("template")
            configNames   = self.getOption("config")
            page_size   = self.getOption("page_size")
            cache   = self.getOption("cache")
            useIndex = self.getOption("use_index")
            new_graph = self.getSingleOption("new")
            size = self.getOption("size")
            threads = self.getOption("threads")
            txsize = self.getOption("txsize")
            cache = self.getOption("cache")
            self.run_operation(rootPath,template,configNames,page_size,cache,useIndex,new_graph,size,threads,txsize)
        return False

        
