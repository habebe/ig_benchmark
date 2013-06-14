import getopt
import sys
import types
import math
import os
import db

class argument:
    def __init__(self,name,dataType,defaultValue,description):
        self.name = name
        self.dataType = dataType
        self.defaultValue = defaultValue
        self.description = description
        pass
    pass

def pow_x(base,start,stop,step=1):
    result = []
    current = start
    while current < stop:
        result.append(int(math.ceil(pow(base,current))))
        current += step
        pass
    return result

def pow_2(start,stop,step=1):
    return pow_x(2,start,stop,step)

def pow_10(start,stop,step=1):
    return pow_x(10,start,stop,step)


class Colors:
    Black  = 30
    Blue   = 34
    Green  = 32
    Cyan   = 36
    Red	   = 31
    Purple = 35
    Brown  = 33
    Blue   = 34
    Green  = 32
    Cyan   = 36
    Red	   = 31
    Purple = 35
    Brown  = 33
    pass


def _hilite_(string, status, bold):
    attr = []
    if status == 1:
        # green
        attr.append('32')
    elif status == 0:
        # red
        attr.append('31')
        pass
    else:
        attr.append(str(status))
        pass
    if bold:
        attr.append('1')
        pass
    return '\x1b[%sm%s\x1b[0m' % (';'.join(attr), string)


class operation:
    def operation_update_database(self,db):
        if self.is_runnable():
            return db.create_unique_object(db_model.case_type,
                                           "name",self.name,
                                           description=self.__doc__
                                           )
        return None
    
    def get_name(self):
        return self.__module__.split(".")[1]

    def __init__(self,name,useDefaultArguments=True):
        if name == None:
            self.name = self.get_name()
        else:
            self.name = name
        self.options = []
        self.arguments = []
        self.argumentDescription = []
        self.optionsMap = {}
        self.argumentMap = {}
        if useDefaultArguments:
            self.add_argument("help",None,None,"show help message")
            self.add_argument("verbose","int",0,"verbose level")
            self.add_argument("name","str",db.db.default_name,"name of benchmark")
            pass
        pass

    def add_argument(self,name,dataType,defaultValue,description):
        a = argument(name,dataType,defaultValue,description)
        self.argumentDescription.append(a)
        self.argumentMap[name] = a
        return


    def is_runnable(self):
        return False

    def mkdir(self,name):
        try:
            if self.verbose > 1:
                print "Creating directory ",name
            os.mkdir(name)
        except Exception,e:
            if self.verbose > 1:
                print e
            pass
        pass
        
    def output_string(self,string, status, bold):
        return _hilite_(string,status,bold)

    def error(self,message):
        print >> sys.stderr,_hilite_("[Error]",False,True),_hilite_(message,False,False)
        return

    def warn(self,message):
        print >> sys.stderr,_hilite_("[Warning]",False,True),_hilite_(message,False,False)
        return
    
    def usage(self,fileName):
        self.usage_options()
        print >> sys.stdout
        pass

    def usage_options(self):
        if self.is_runnable():
             print "\t<%s>"%(self.output_string(self.name,Colors.Blue,True)),
        else:
            print >> sys.stdout,"\t<%s>"%(self.name),    
            pass
        if self.__doc__:
            print self.__doc__
        else:
            print
            pass
        print >> sys.stdout,"\t<options>"
        for i in self.argumentDescription:
            if i.dataType == None:
                print >> sys.stdout,"\t\t[--%s] \t%s"%(i.name,str(i.description))
            else:
                print >> sys.stdout,"\t\t[--%s] \t<%s> %s <default:%s>"%(i.name,i.dataType,str(i.description),str(i.defaultValue))
        pass

    def addOption(self,name,value):
        if not self.optionsMap.has_key(name):
            self.optionsMap[name] = []
        self.optionsMap[name].append(value)
        pass

    def hasOption(self,name):
        return self.optionsMap.has_key(name)


    def getOption_data(self,data,name):
        if data.has_key(name):
            return data[name]
        return self.getOption(name)

    def getOption(self,name,combine = True):
        if self.hasOption(name):
            value = self.optionsMap[name]
            if combine:
                result = []
                for i in value:
                    if type(i) == types.ListType:
                        result = result + i
                    else:
                        result.append(i)
                    pass
                return result
            else:
                return value
        if self.argumentMap.has_key(name):
            argument = self.argumentMap[name]
            if argument.defaultValue:
                exp = "[%s('%s')]"%(argument.dataType,str(argument.defaultValue))
                return eval(exp)
        return None

    def getSingleOption(self,name):
        value = self.getOption(name)
        if value == None:
            return value
        return value[0]

        
    def parse(self,arguments):
        argumentNameList = []
        for i in self.argumentDescription:
            if i.dataType == None:
                argumentNameList.append("%s"%(i.name))
            else:
                argumentNameList.append("%s="%(i.name))
                pass
            pass
       
        try:
            self.options, self.arguments = getopt.getopt(arguments, "",argumentNameList)
            for o,a in self.options:
                for arg in self.argumentDescription:
                    argName = "--%s"%(arg.name)
                    if o in (argName):
                        if arg.dataType == None:
                            self.addOption(arg.name,1)
                        else:
                            expression = "%s('%s')"%(arg.dataType,a)
                            value = eval(expression)
                            self.addOption(arg.name,value)
                            pass
                        pass
                    pass
                pass
            return True
        except Exception as e:
            self.error(e)
            return False
        pass

    def operate(self):
        self.verbose = self.getSingleOption("verbose")
        if self.verbose == None:
            self.verbose = 0
        if self.hasOption("help"):
            self.usage(None)
            return 0
        self.name = self.getSingleOption("name")
        return 1
    pass


operations = {}
import run_operation
import init_operation

if 0:
    import query
    import show_tables
    import run
    import delete_tag
    import report
#import show_db_config
    import graph_create
    import graph_v_ingest
    import graph_v_search
    import make_graph500
    import generate_elist
    import graph_e_ingest
    import graph_e_standard_ingest
    import graph_e_ring_ingest
    import graph_navigate_ring
    import graph_navigate_dense
    
def add_operation(operation):
    operations[operation.name] = operation
    pass

def populate():
    if len(operations) == 0:
        add_operation(run_operation.operation())
        add_operation(init_operation.operation())
        pass
    pass

def _operation_(name):
    populate()
    name = name.lower()
    if operations.has_key(name):
        return operations[name]
    return None



