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
                                        name="vertex_ingest",
                                        txtype="write",
                                        optypes=["op.V"])
        self.remove_argument("vertex")
        pass
    pass


