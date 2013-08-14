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
import ig_operation

class operation(ig_operation.operation):
    def __init__(self):
        ig_operation.operation.__init__(self,
                                        name="composite_ingest",
                                        txtype="write",
                                        optypes=["op.C"])
        self.remove_argument("vertex")
        self.add_argument("composite_name","str",None,"Name of the composite element in the template.")
        pass

    def run(self,db,suite,case,data,**kwargs):
        self.composite_name = self.getOption_data(data,"composite_name")
        if self.composite_name == None:
            self.error("Composite name is not given.")
            return False
        return ig_operation.operation.run(self,db,suite,case,data,**kwargs)

    def operate(self):        
        if not self.hasOption("help"):
            self.composite_name = self.getSingleOption("composite_name")
            if not self.composite_name:
                self.error("composite_name is not given.")
                return False
            pass
        return ig_operation.operation.operate(self)
    
    pass


