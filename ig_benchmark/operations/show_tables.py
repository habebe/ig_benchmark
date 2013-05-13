import os
import sys
import getopt
import base
import db_objects

class operation(base.operation):
    def __init__(self):
        base.operation.__init__(self)
        self.add_argument("name","str",db_objects.db.default_name,"name of benchmark")
        self.add_argument("detail","int",0,"detail level")
        pass

    def operate(self):
        if base.operation.operate(self):
            self.dbname = self.getSingleOption("name")
            self.detail = self.getSingleOption("detail")
            db = db_objects.db(self.dbname)
            if not db.exists():
                self.error("Benchmark data does not exist.")
                self.error("\tLooking for '%s'."%(db.name))
                return False
            fromTable = "sqlite_master"
            where = "type = 'table'"
            select = "*"
            (errorMessage,data) = db.raw_query(select,fromTable,where)
            print self.output_string("Table names",base.Colors.Purple,True)
            
            if errorMessage == None:
                data = data[1]
                for i in data:
                    if self.detail:
                        print "\n\t",self.output_string(str(i[1]),base.Colors.Blue,False)
                        statement = i[4]
                        start_index = statement.find("(")
                        statement = statement[start_index+1:len(statement)-1]
                        members = statement.split(",")
                        for m in members:
                            print "\t\t",self.output_string(str(m),base.Colors.Cyan,False)
                    else:
                        print "\t",self.output_string(str(i[1]),base.Colors.Blue,False)
            else:
                self.error(errorMessage)
                pass
        return False
