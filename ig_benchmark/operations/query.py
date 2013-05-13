import os
import sys
import getopt
import base
import db_objects
import types

class operation(base.operation):
    "Query benchmark/test database."
    def __init__(self):
        base.operation.__init__(self)
        self.add_argument("name","str",db_objects.db.default_name,"name of benchmark")
        self.add_argument("select","str","*","table members to select.")
        self.add_argument("from","str",None,"table name.")
        self.add_argument("where","str",None,"sql condition.")
        pass

    def operate(self):
        if base.operation.operate(self):
            self.dbname = self.getSingleOption("name")
            self.select = self.getSingleOption("select")
            self.fromTable  = self.getSingleOption("from")
            self.where = self.getSingleOption("where")
            db = db_objects.db(self.dbname)
            print self.output_string("Query results",base.Colors.Purple,True)
            (errorMessage,data) = db.raw_query(self.select,self.fromTable,self.where)
            if errorMessage == None:
                description = data[0]
                data = data[1]
                counter = len(description)
                print "\n--"
                for d in description:
                    print self.output_string(d[0],base.Colors.Blue,True),
                    if counter > 1:
                        print "|",
                        pass
                    counter -= 1
                    pass
                print "\n--"
                for row in data:
                    counter = len(row)
                    for item in row:
                        if (type(item) == types.StringType) or (type(item) == types.UnicodeType):
                            print self.output_string('"%s"'%(item),base.Colors.Blue,False), 
                        else:
                            print self.output_string(item,base.Colors.Blue,False),
                            pass
                        if counter > 1:
                            print "|",
                            pass
                        counter -= 1
                        pass
                    print
                print "--\n"
                pass
            else:
                self.error(errorMessage)
                pass
        return False
    pass
