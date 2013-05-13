import db_benchmark
import os
import base
import time
import sys
import db_objects
import random
import types

class operation(db_benchmark.operation):
    def __init__(self):
        db_benchmark.operation.__init__(self)
        self.add_argument("index","str","none","index type")
        self.add_argument("size","eval",pow(2,12),"number of vertices.")
        self.add_argument("search_size","eval",None,"The number of vertices to search for.")
        self.add_argument("seed","int",0,"Random seed used in generating search list.")
        self.add_argument("threads","eval",1,"number of threads")
        self.add_argument("txsize","eval",10000,"transaction size")
        self.add_argument("cache","eval",(1000,500000),"cache size given as a set of tuplets (in kB) (init,max) or [(init_1,max_1),(init_2,max_2),.....]")
        self.tag_object = None
        self.case_object = None
        self.cache = None
        pass

    def generate_random_searchlist(self,fileName,randomGenerator,size,graphSize):
        searchListFile = file(fileName,"w")
        for i in xrange(size):
            print >> searchListFile,randomGenerator.randint(1,graphSize)
            pass
        searchListFile.flush()
        searchListFile.close()
        pass
    
    def run(self,db,suite,case,data,**kwargs):
        self.case_object = case
        self.verbose = kwargs["verbose"]
        self.db = db
        suite_problem_size = suite.get_problem_size()
        problem_size = kwargs["problem_size"]
        if suite_problem_size:
            current_problem_size = suite_problem_size[problem_size]
            if current_problem_size:
                for i in current_problem_size:
                    data[str(i)] = current_problem_size[str(i)]
                    pass
                pass
            pass
        self.diskmap = self.getOption_data(data,"diskmap")
        self.hostmap = self.getOption_data(data,"hostmap")
        self.engine = self.getOption_data(data,"engine")
        self.index = self.getOption_data(data,"index")
        self.page_size = self.getOption_data(data,"page_size")
        self.txsize = self.getOption_data(data,"txsize")
        self.threads = self.getOption_data(data,"threads")
        self.graph_size = self.getOption_data(data,"graph_size")[0]
        self.cache = self.getOption_data(data,"cache")
        self.search_size = self.getOption_data(data,"search_size")
        self.seed = self.getOption_data(data,"seed")
        self.setup_engine_objects()
        self.setup_page_sizes(self.page_size)
        self.tag_object = self.db.create_unique_object(db_objects.model.tag,"name",kwargs["tag"],
                                                       timestamp=self.db.now_string(True))
        self.run_operation()
        pass

    def os_remove_profile_files(self):
        listing = os.listdir(".")
        for i in listing:
            if i.endswith(".profile"):
                try:
                    os.remove(i)
                except Exception as e:
                    print e
                    pass
                pass
            pass
        pass

    def run_operation(self):
        random.seed(self.seed)
        for _index in self.index: 
            for _page_size in self.page_size:
                for _v_size_value in self.search_size:
                    for _threads in self.threads:
                        for _txsize in self.txsize:
                            for _cache in self.cache:
                                for engine in self.engine_objects:
                                    _v_size = None
                                    _v_size_set = None
                                    if type(_v_size_value) == types.ListType:
                                        _v_size = _v_size_value[0]
                                        _v_size_set = _v_size_value[1]
                                    else:
                                        _v_size = _v_size_value
                                        _v_size_set = self.graph_size
                                        pass
                                    #boot_file_path = self.config.BootFilePath[engine.name]
                                    #boot_file_name = os.path.join(boot_file_path,"bench.boot")
                                    search_file_name = "searchlist.data"
                                    self.generate_random_searchlist(search_file_name,random,_v_size,_v_size_set)
                                    print
                                    self.initialize_property(engine.name)
                                    self.propertyFile.setInitCache(_cache[0])
                                    self.propertyFile.setMaxCache(_cache[1])
                                    self.propertyFile.properties["IG.PageSize"] = _page_size
                                    _s_ = "\tgraph(%s) search vertices index:%s page_size:%d tx_size:%d size:%d search_size:%d search_size_set:%d cache_init:%d cache_max:%d search_ratio=%f"%(
                                        engine.name,_index,_page_size,_txsize,self.graph_size,_v_size,_v_size_set,_cache[0],_cache[1],(1.0*_v_size/_v_size_set)
                                        )
                                    print self.output_string(_s_,base.Colors.Blue,True)
                                    profileName = "search.profile"
                                    if self.tag_object:
                                        profileName = self.tag_object.name +"_"+ self.db.now_string(True)  +"_"+ ".profile"
                                        profileName = profileName.replace(" ","_")
                                        pass

                                    
                                    self.ig_v_search(search_file_name,engine.name,self.propertyFile,_index,_v_size,self.graph_size,_threads,_txsize,profileName)
                                    if self.case_object:
                                        if os.path.exists(profileName):
                                            f = file(profileName,"r")
                                            line = f.readline()
                                            data = None
                                            try:
                                                data = eval(line)
                                            except Exception as e:
                                                print e
                                                data = None
                                                self.os_remove_profile_files()
                                                #os.remove(profileName)
                                                pass
                                            if data != None:
                                                if (_v_size == data["opsize"]):
                                                    platform_object = self.db.create_unique_object(db_objects.model.platform,"name",data["os"])
                                                    index_object = self.db.create_unique_object(db_objects.model.index_type,"name",_index)
                                                    
                                                    assert(_v_size == data["opsize"])
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
                                                    if _v_size_set != self.graph_size:
                                                        case_data_object.setDataValue("search_set_size",_v_size_set)
                                                        self.db.update(case_data_object)
                                                        pass
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
                                                    self.os_remove_profile_files()
                                                    #os.remove(profileName)
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
    
    def operate(self):
        if db_benchmark.operation.operate(self):
            self.index = self.getOption("index")
            self.graph_size = self.getSingleOption("size")
            self.threads = self.getOption("threads")
            self.txsize = self.getOption("txsize")
            self.cache = self.getOption("cache")
            self.search_size = self.getOption("search_size")
            self.seed = self.getSingleOption("seed")
            self.seed = self.getSingleOption("seed")
            self.run_operation()
        return False

        
