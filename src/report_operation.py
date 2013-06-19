import os
import sys
import getopt
import operations
import db
import db_report

class operation(operations.operation):
    "Generates a set of web pages to report the state of the database."
    def __init__(self):
        operations.operation.__init__(self,"report")
        self.add_argument("name","str",db.db.default_name,"name of benchmark")
        self.add_argument("location","str",db_report.default_location,"name of the directory to save the benchmark web report.")
        pass

    def operate(self):
        if operations.operation.operate(self):
            self.dbname = self.getSingleOption("name")
            self.location = self.getSingleOption("location")
            self.db = db.db()
            obj = db_report.db_report(self.db)
            return obj.report(self,self.location)
        return False
