import db_benchmark
import os
import base
import time
import sys

class operation(db_benchmark.operation):
    def __init__(self):
        db_benchmark.operation.__init__(self)
        self.add_argument("index","str","none","index type")
    
        pass

    def run(self,db,suite,case,data,**kwargs):
        print "run"
        pass

    def run_operation(self):
        for engine in self.engine_objects:
            for _index in self.index: 
                self.initialize_property(engine.name)
                for _page_size in self.page_size:
                    self.propertyFile.properties["IG.PageSize"] = _page_size
                    self.ig_setup_placement(engine.name,self.propertyFile)
                    _s_ = "\tcreate graph(%s) index:%s page_size:%d"%(engine.name,_index,_page_size)
                    print self.output_string(_s_,base.Colors.Blue,True),
                    sys.stdout.flush()
                    start = time.clock()
                    self.ig_run(engine.name,self.propertyFile,"create",_index)
                    elapsed = (time.clock() - start)
                    print self.output_string(str(elapsed),base.Colors.Red,False)
                    self.ig_setup_Location(engine.name,self.propertyFile)
                    pass
                pass
            pass
        pass

    def operate(self):
        if db_benchmark.operation.operate(self):
            self.index = self.getOption("index")
            self.run_operation()
        return False

        
