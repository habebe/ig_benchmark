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
import benchmark_runner
import types

class operation(operations.operation):
    def __init__(self,name,txtype,optypes):
        operations.operation.__init__(self,name)
        self.add_argument("root","str",".","Root path.")

        self.add_argument("vertex","str",None,"vertex name to query for")
        self.add_argument("template","str",None,"template name")
        self.add_argument("page_size","int","14","page size to use applicable when a new graph is created (use pows of two) (12 for 4K up to 16 for 64K)")
        self.add_argument("config","str",None,"config name")

        self.add_argument("use_index","int",1,"use index")
        self.add_argument("new","int",1,"create new graph")

        self.add_argument("graph_size","eval",100000,"graph size")
        self.add_argument("size","eval",100000,"operation size")

        self.add_argument("threads","eval",1,"number of threads")

        self.add_argument("txsize","eval",10000,"transaction size")
        self.add_argument("txlimit","int",-1,"limit the number of transactions to a given number")
        self.add_argument("cache","eval",(1000,500000),"cache size given as a set of tuplets (in kB) (init,max) or [(init_1,max_1),(init_2,max_2),.....]")
        self.add_argument("ig_version","str",None,"InfiniteGraph version to use.")
        self.add_argument("process","eval",'(None,1)',"concurrent processes to use given using the format [(host,numberOfProcesses),(host2,numberOfProcess),...] default is [(127.0.0.1,1)] i.e. local host with 1 process.")
        self.txtype = txtype
        self.optypes = optypes
        self.tag_object = None
        self.case_object = None
        if self.txtype == None:
            self.txtype = "write"
            pass
        pass

    def is_runnable(self):
        return True


    def run(self,db,suite,case,data,**kwargs):
        self.case_object = case
        self.verbose = kwargs["verbose"]
        self.db = db
        self.rootPath = os.path.dirname(db_model.suite.RootSuite.get_path())
        self.template     = self.getOption_data(data,"template")
        self.configNames  = self.getOption_data(data,"config")
        self.page_size    = self.getOption_data(data,"page_size")
        self.cache        = self.getOption_data(data,"cache")
        self.use_index     = self.getOption_data(data,"use_index")
        self.new_graph    = self.getOption_data(data,"new")
        self.graph_size   = self.getOption_data(data,"graph_size")
        self.size         = self.getOption_data(data,"size")
        self.threads      = self.getOption_data(data,"threads")
        self.txsize       = self.getOption_data(data,"txsize")
        self.txlimit      = self.getOption_data(data,"txlimit")
        self.cache        = self.getOption_data(data,"cache")
        self.ig_version   = self.getOption_data(data,"ig_version")
        self.process      = self.getOption_data(data,"process")
        self.vertex       = self.getOption_data(data,"vertex")
        self.tag_object = self.db.create_unique_object(db_model.tag,"name",kwargs["tag"],
                                                       timestamp=self.db.now_string(True))
        if type(self.new_graph) != types.ListType:
            self.new_graph = [self.new_graph]
            pass
        if type(self.use_index) != types.ListType:
            self.use_index = [self.use_index]
            pass
        if type(self.vertex) != types.ListType:
            self.vertex = [self.vertex]
            pass
        #self.run_operation(ig_version,rootPath,template,configNames,page_size,cache,useIndex,new_graph,size,threads,txsize,txlimit,process)
        return self._run_()
        
    
    def operate(self):
        if operations.operation.operate(self):
            self.rootPath      = self.getSingleOption("root")
            self.template      = self.getOption("template")
            self.configNames   = self.getOption("config")
            self.page_size     = self.getOption("page_size")
            self.cache         = self.getOption("cache")
            self.use_index      = self.getOption("use_index")
            self.new_graph     = self.getOption("new")
            self.size          = self.getOption("size")
            self.graph_size    = self.getOption("graph_size")
            self.threads       = self.getOption("threads")
            self.txsize        = self.getOption("txsize")
            self.txlimit       = self.getOption("txlimit")
            self.cache         = self.getOption("cache")
            self.ig_version    = self.getOption("ig_version")
            self.process       = self.getOption("process")
            self.vertex        = self.getOption("vertex")
            return self._run_()
            #return self.run_operation(ig_version,rootPath,template,configNames,page_size,cache,useIndex,new_graph,size,threads,txsize,txlimit)
        return False


    def _run_(self):
        if self.template == None:
            self.error("template is required but not given.")
            return False
        if self.configNames == None:
            self.error("config name is required but not given.")
            return False
        if self.ig_version == None:
            self.error("InfiniteGraph version is not given.")
            return False
        if self.rootPath == None:
            self.error("Root path is not given.")
            return False
        self.rootPath = os.path.abspath(self.rootPath)
        runners = []
        for iVersion in self.ig_version:
            working_path = self.setupWorkingPath(self.rootPath,iVersion)
            for iTemplate in self.template:
                for iGraphSize in self.graph_size:
                    for iSize in self.size:
                        for iConfig in self.configNames:
                            for iPageSize in self.page_size:
                                for iCache in self.cache:
                                    for iUseIndex in self.use_index:
                                        for iThreads in self.threads:
                                            for iTxSize in self.txsize:
                                                for iTxLimit in self.txlimit:
                                                    for iProcess in self.process:
                                                        for iNewGraph in self.new_graph:
                                                            for iUseIndex in self.use_index:
                                                                for iVertex in self.vertex:
                                                                    runner = benchmark_runner.benchmark_runner(_working_path=working_path,
                                                                                                               _root_path=self.rootPath,
                                                                                                               _operation=self,
                                                                                                               _new_graph=iNewGraph,
                                                                                                               _version=iVersion,
                                                                                                               _template=iTemplate,
                                                                                                               _config=iConfig,
                                                                                                               _size=iSize,
                                                                                                               _graph_size=iGraphSize,
                                                                                                               _page_size=iPageSize,
                                                                                                               _cache_size=iCache,
                                                                                                               _use_index=iUseIndex,
                                                                                                               _threads=iThreads,
                                                                                                               _tx_size=iTxSize,
                                                                                                               _tx_limit=iTxLimit,
                                                                                                               _tx_type=self.txtype,
                                                                                                               _vertex=iVertex,
                                                                                                               _process=iProcess
                                                                                                               )
                                                                    runners.append(runner)
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
                    pass
                pass
            pass
        counter   = 1
        totalSize = len(runners) 
        for i in runners:
            i.message(counter,totalSize)
            if i.setup():
                i.operate()
                if i.return_code == 0:
                    self.updateDatabase(i)
                    pass
                pass
            counter += 1
            pass
        return True

    def __shouldPresistProfile__(self,profile_data):
        if self.optypes == None:
            return True
        if type(self.optypes) == types.ListType:
            for i in self.optypes:
                if profile_data.has_key(i):
                    return True
                pass
            return False
        try:
            return profile_data.has_key(self.optypes)
        except:
            return False
        return False
    
    def updateDatabase(self,runner):
        if not self.db:
            return
        if (runner.profile == None) or (len(runner.profile) == 0):
            self.error("Current run does not contain any benchmark data.")
            return
        engine_object = self.db.create_unique_object(db_model.engine,"name",runner.engine.version,description=runner.engine.version)
        config_object = self.db.create_unique_object(db_model.config,"name",runner.configObject.getFullName(),description=runner.configObject.getFullName())
        if self.case_object:
            for runner_profile in runner.profile:
                profile_data = runner_profile["data"]
                if self.__shouldPresistProfile__(profile_data):
                    platform_object = self.db.create_unique_object(db_model.platform,"name",self.db.hostname(),type=runner_profile["os"])
                    if runner.use_index:
                        index_object = self.db.create_unique_object(db_model.index_type,"name","gr")
                    else:
                        index_object = self.db.create_unique_object(db_model.index_type,"name","none")
                        pass
                    process_description_object = self.db.create_unique_object(db_model.process_description,"name",runner.getProcessDescription(),description=runner.getProcessDescription())
                    case_data_object = self.db.create_object(db_model.case_data,
                                                             timestamp=self.db.now_string(True),
                                                             case_id=self.case_object.id,
                                                             engine_id=engine_object.id,
                                                             tag_id=self.tag_object.id,
                                                             size=profile_data["size"],
                                                             time=runner_profile["time"],
                                                             memory_init=runner_profile["memInit"],
                                                             memory_used=runner_profile["memUsed"],
                                                             memory_committed=runner_profile["memCommitted"],
                                                             memory_max=runner_profile["memMax"],
                                                             rate=profile_data["rate"],
                                                             page_size=runner.page_size,
                                                             cache_init=runner.propertyFile.getInitCache(),
                                                             cache_max=runner.propertyFile.getMaxCache(),
                                                             tx_size=runner.tx_size,
                                                             platform_id=platform_object.id,
                                                             threads=runner.threads,
                                                             processes=runner.number_processes,
                                                             process_description_id=process_description_object.id,
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
                    case_data_stat_object.setTimeStat(runner_profile["time"])
                    case_data_stat_object.setMemInitStat(runner_profile["memInit"])
                    case_data_stat_object.setMemUsedStat(runner_profile["memUsed"])
                    case_data_stat_object.setMemCommittedStat(runner_profile["memCommitted"])
                    case_data_stat_object.setMemMaxStat(runner_profile["memMax"])
                    self.db.update(case_data_stat_object)
                    pass
                pass
            pass
        pass
    
   
        
