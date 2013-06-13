import os
import sys
import getopt
import base
import db_objects

class operation(base.operation):
    def __init__(self):
        base.operation.__init__(self)
        self.add_argument("name","str",db_objects.db.default_name,"name of benchmark")
        self.add_argument("where","str",None,"sql condition.")
        pass

    def operate(self):
        if base.operation.operate(self):
            self.dbname = self.getSingleOption("name")
            self.where = self.getSingleOption("where")    
            db = db_objects.db(self.dbname)
            data = db.delete_tag(self.where)
            errorMessage = data[0]
            data = data[1]
            if errorMessage == None:
                if self.where == None:
                    print self.output_string("Delete all tag",base.Colors.Purple,True)
                else:
                    print self.output_string("Delete tag where (%s)"%(str(self.where)),base.Colors.Purple,True)
                    pass
                for row in data:
                    print "\t",self.output_string("[deleted] ",base.Colors.Red,True),
                    print self.output_string(str(row[0]),base.Colors.Red,False),":",
                    print self.output_string(str(row[1]),base.Colors.Red,False)
                    pass
                pass
            else:
                self.error(errorMessage)
        return False
