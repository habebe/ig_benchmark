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

class operation(operations.operation):
    def __init__(self):
        operations.operation.__init__(self,"edge_ingest")
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
        runners = []
        for iVersion in ig_version:
            for iTemplate in template:
                for iSize in size:
                    working_path = self.setupWorkingPath(rootPath,iVersion)
                    for iConfig in configNames:
                        for iPageSize in page_size:
                            for iCache in cache:
                                for iUseIndex in useIndex:
                                    for iThreads in threads:
                                        for iTxSize in txsize:
                                            for iTxLimit in txlimit:
                                                runner = benchmark_runner.benchmark_runner(working_path,rootPath,self,
                                                                                           new_graph,iVersion,iTemplate,iConfig,iSize,iSize,
                                                                                           iPageSize,iCache,iUseIndex,iThreads,iTxSize,iTxLimit,
                                                                                           "write")
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
                if profile_data.has_key("op.E"):
                    print "\t\t\tEdge ingest rate:",profile_data["rate"]," Size:",profile_data["size"]
                    platform_object = self.db.create_unique_object(db_model.platform,"name",self.db.hostname(),type=runner_profile["os"])
                    if runner.use_index:
                        index_object = self.db.create_unique_object(db_model.index_type,"name","gr")
                    else:
                        index_object = self.db.create_unique_object(db_model.index_type,"name","none")
                        pass
                    
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

        
