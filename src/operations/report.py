import os
import sys
import getopt
import base
import db_objects

class operation(base.operation):
    "Generates a set of web pages to report the state of the database."
    def __init__(self):
        base.operation.__init__(self)
        self.add_argument("name","str",db_objects.db.default_name,"name of benchmark")
        self.add_argument("location","str",db_objects.db_report.default_location,"name of the directory to save the benchmark web report.")
        pass

    def operate(self):
        if base.operation.operate(self):
            self.dbname = self.getSingleOption("name")
            self.location = self.getSingleOption("location")
            self.db = db_objects.db(self.dbname)
            db_report = db_objects.db_report.db_report(self.db)
            return db_report.report(self,self.location)
        return False
