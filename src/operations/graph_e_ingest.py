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
        self.add_argument("factor","eval",4,"edge factor")
        self.add_argument("a","float",0.25,"a")
        self.add_argument("b","float",0.25,"b")
        self.add_argument("c","float",0.25,"c")
        self.add_argument("d","float",0.25,"d")
        self.add_argument("threads","eval",1,"number of threads")
        self.add_argument("txsize","eval",10000,"transaction size")
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
        self.factor = self.getOption_data(data,"factor")
        self.a = self.getOption_data(data,"a")
        self.b = self.getOption_data(data,"b")
        self.c = self.getOption_data(data,"c")
        self.d = self.getOption_data(data,"d")
        self.threads = self.getOption_data(data,"threads")
        
        self.diskmap = self.getOption_data(data,"diskmap")
        self.hostmap = self.getOption_data(data,"hostmap")
        self.engine = self.getOption_data(data,"engine")
        self.page_size = self.getOption_data(data,"page_size")
        self.txsize = self.getOption_data(data,"txsize")
        self.cache = self.getOption_data(data,"cache")

        self.setup_engine_objects()
        self.setup_page_sizes(self.page_size)
        self.tag_object = self.db.create_unique_object(db_objects.model.tag,"name",kwargs["tag"],
                                                       timestamp=self.db.now_string(True))
        self.run_operation()
        pass

    def run_operation(self):
        for engine in self.engine_objects:
            for _page_size in self.page_size:
                for _v_scale in self.scale:
                    for _e_factor in self.factor:
                        for _threads in self.threads:
                            for _txsize in self.txsize:
                                for _cache in self.cache:
                                    _index = "gr"
                                    _v_size = pow(2,_v_scale)
                                    self.initialize_property(engine.name)
                                    self.propertyFile.properties["IG.Placement.Distributed.Pipelining.Groups"] = "ConnectorGroup"
                                    self.propertyFile.properties["IG.Placement.Distributed.Pipelining.PipelinesPerStorageUnit"]=4
                                    self.propertyFile.properties["IG.Placement.Distributed.Pipelining.EnablePipelining"]="true"
                                    self.propertyFile.setInitCache(_cache[0])
                                    self.propertyFile.setMaxCache(_cache[1])
                                    self.propertyFile.properties["IG.PageSize"] = _page_size
                                    self.ig_setup_placement(engine.name,self.propertyFile)
                                    _s_ = "\tgraph(%s) create index:%s page_size:%d"%(engine.name,_index,_page_size)
                                    print self.output_string(_s_,base.Colors.Blue,True),
                                    sys.stdout.flush()
                                    start = time.clock()
                                    self.ig_run(engine.name,self.propertyFile,"create",_index)
                                    elapsed = (time.clock() - start)
                                    print self.output_string(str(elapsed),base.Colors.Red,False)
                                    self.ig_setup_Location(engine.name,self.propertyFile)
                                

                                    _s_ = "\tgraph(%s) ingest edges index:%s page_size:%d tx_size:%d size:%d diskmap:%s"%(engine.name,_index,_page_size,_txsize,_v_size,str(self.diskmap))
                                    print self.output_string(_s_,base.Colors.Blue,True)
                                    vprofileName = "v_ingest.profile"
                                    eprofileName = "e_ingest.profile"
                                    elist_name  = "g500.elist"
                                    now_string = self.db.now_string(True).replace(" ","_")
                                    if self.tag_object:
                                        vprofileName = "v_ingest."+ now_string  +  ".profile"
                                        eprofileName = "e_ingest."+ now_string +  ".profile"
                                        vprofileName = vprofileName.replace(" ","_")
                                        eprofileName = eprofileName.replace(" ","_")
                                        elist_name = "g500.%s.elist"%(now_string)
                                        pass
                                    self.ig_v_ingest(engine.name,self.propertyFile,_index,_v_size,0,_threads,_txsize,vprofileName,True)
                                    generator = generate_elist.operation()
                                    generator.run(_v_scale,_e_factor,self.a,self.b,self.c,self.d,elist_name)
                                    self.ig_e_pipeline_ingest(engine.name,self.propertyFile,_v_scale,_threads,_txsize,eprofileName,elist_name)
                                    os.remove(elist_name)
                                    if self.case_object:
                                        f = file(eprofileName,"r")
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
                                        os.remove(eprofileName)
                                        os.remove(vprofileName)
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
            self.scale = self.getOption("scale")
            self.factor = self.getOption("factor")
            self.a = self.getSingleOption("a")
            self.b = self.getSingleOption("b")
            self.c = self.getSingleOption("c")
            self.d = self.getSingleOption("d")
            self.threads = self.getOption("threads")
            self.txsize = self.getOption("txsize")
            self.cache = self.getOption("cache")
            self.run_operation()
        return False

        
