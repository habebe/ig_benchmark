import db_benchmark
import os
import base
import time
import sys
import db_objects
import generate_elist

class operation(db_benchmark.operation):
    def __init__(self):
        db_benchmark.operation.__init__(self)
        self.add_argument("scale","eval",8,"scale")
        self.add_argument("type","str","dfs","navigate type [dfs|bfs]")
        self.add_argument("cache","eval",(1000,500000),"cache size given as a set of tuplets (in kB) (init,max) or [(init_1,max_1),(init_2,max_2),.....]")
        self.tag_object = None
        self.case_object = None
        self.cache = None
        self.map_oid = False
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
        self.scale = self.getOption_data(data,"scale")
        self.type = self.getOption_data(data,"type")
        self.diskmap = self.getOption_data(data,"diskmap")
        self.hostmap = self.getOption_data(data,"hostmap")
        self.engine = self.getOption_data(data,"engine")
        self.page_size = self.getOption_data(data,"page_size")
        self.cache = self.getOption_data(data,"cache")

        self.setup_engine_objects()
        self.setup_page_sizes(self.page_size)
        self.tag_object = self.db.create_unique_object(db_objects.model.tag,"name",kwargs["tag"],
                                                       timestamp=self.db.now_string(True))
        self.run_operation()
        pass

    def run_operation(self):
        for engine in self.engine_objects:
            for _cache in self.cache:
                _index = "gr"
                _v_scale = self.scale
                _v_size = pow(2,_v_scale)
                self.initialize_property(engine.name)
                self.propertyFile.setInitCache(_cache[0])
                self.propertyFile.setMaxCache(_cache[1])
                navigate_profileName = "navigate.profile"
                now_string = self.db.now_string(True).replace(" ","_")
                if self.tag_object:
                    navigate_profileName = "navigate."+ now_string  +  ".profile"
                    pass
                f = file("ring_search.txt","w")
                print >> f,"0,0"
                f.flush()
                f.close()
                self.ig_nagivate(self.type,engine.name,self.propertyFile,_index,_v_size,0,_threads,_txsize,navigate_profileName,"ring_search.txt")
                if self.case_object:
                    f = file(navigate_profileName,"r")
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
                        pass
                    case_data_object.setDataValue("edge_factor",_e_factor)
                    self.db.update(case_data_object)
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
                    os.remove(navigate_profileName)
                    pass
                pass
            pass
        pass
    
    def operate(self):
        if db_benchmark.operation.operate(self):
            self.scale = self.getSingleOption("scale")
            self.type = self.getSingleOption("type")
            self.threads = self.getOption("threads")
            self.txsize = self.getOption("txsize")
            self.cache = self.getOption("cache")
            self.run_operation()
        return False

        
