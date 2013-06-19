import getopt
import sys
import types
import math
import os
import db
import db_model
import socket
import config
import platform

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
    Options = None
    Hostname = socket.gethostname()
    
    @classmethod
    def LoadOptions(self,rootPath):
        if self.Options == None:
            optionsPath = os.path.join(rootPath,"options.py")
            if os.path.exists(optionsPath):
                f = file(optionsPath,"r")
                line = f.read()
                try:
                    self.Options = eval(line)
                except:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    message  = "\n\tFile:%s\n"%(optionsPath)
                    message += "\tLine number:%d\n"%(exc_tb.tb_lineno)
                    message += "\t%s\n"%(str(exc_obj))
                    self.error(message)
                    pass
                pass
            pass
        if self.Options == None:
            self.Options = {}
            pass
        pass

    @classmethod
    def GetConfigPath(self,rootPath):
        self.LoadOptions(rootPath)
        if self.Options.has_key(self.Hostname):
            options = self.Options[self.Hostname]
            if options.has_key("config"):
                return os.path.join(rootPath,options["config"])
        return os.path.join(rootPath,"config")


    def getConfigList(self,rootPath,name):
        configParameter = name.split(":")
        configFileName = configParameter[0]
        if not configFileName.endswith(".xml"):
            configFileName = configFileName + ".xml"
            pass
        configName = None
        if len(configParameter) == 2:
            configName = configParameter[1]
            pass
        configFileName = os.path.join(self.GetConfigPath(rootPath),configFileName)
        configList = config.parse(configFileName,self)
        try:
            configList = config.parse(configFileName,self)
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            self.error("Error while parsing config file. {0}".format(exc_obj))
            return None
        return (configList,configName)

    def getEnv(self,ig_version,ig_home):
        env = os.environ.copy()
        if platform.system().lower().find("darwin") >= 0:
            env["IG_HOME"] = os.path.join(ig_home,"mac86_64")
            if env.has_key("DYLD_LIBRARY_PATH"):
                env["DYLD_LIBRARY_PATH"] = "{0}:{1}".format(os.path.join(env["IG_HOME"],"lib"),env["DYLD_LIBRARY_PATH"])
            else:
                env["DYLD_LIBRARY_PATH"] = os.path.join(env["IG_HOME"],"lib")
                pass
            if env.has_key("PATH"):
                env["PATH"] = "{0}:{1}".format(os.path.join(env["IG_HOME"],"bin"),env["PATH"])
            else:
                env["PATH"] = os.path.join(env["IG_HOME"],"bin")
                pass
        else:
            assert 0
            pass
        return env
    
    def setupWorkingPath(self,rootPath,version):
        working_path = os.path.join(rootPath,"working",version)
        if not os.path.exists(working_path):
            os.mkdir(working_path)
            pass
        return working_path
     
    def getEngine(self,configList,ig_version):
        engine = None
        i = 0
        for i in configList.engines:
            if i.version == ig_version:
                return i
            pass
        return None
    
    def getConfigObject(self,rootPath,name):
        configListData = self.getConfigList(rootPath,name)
        configObject = None
        if configListData:
            (configList,configName) = configListData
            if configList and (len(configList.configs) > 0):
                configParameter = name.split(":")
                configFileName = configParameter[0]
                if configName:
                    for i in configList.configs:
                        if i.name == configName:
                            configObject = i
                            pass
                        pass
                    if configObject == None:
                        self.error("Unable to find config with a name {0}.".format(self.configName))
                        return None
                    pass
                else:
                    configObject = configList.configs[0]
                    pass
                pass
            pass
        if configObject:
            return (configList,configObject)
        return None
    
    def removeProfileData(self,working_path):
        events  = os.path.join(working_path,"benchmark.events")
        profile = os.path.join(working_path,"benchmark.profile")
        if os.path.exists(events):
            os.remove(events)
            pass
        if os.path.exists(profile):
            os.remove(profile)
            pass
        pass

    def readProfileData(self,working_path):
        eventsName  = os.path.join(working_path,"benchmark.events")
        profileName = os.path.join(working_path,"benchmark.profile")
        events  = []
        profile = None
        if os.path.exists(eventsName):
            f = file(eventsName,"r")
            line = f.readline()
            while len(line):
                line = eval(line)
                events.append(line)
                line = f.readline()
                pass
            pass
        if os.path.exists(profileName):
            f = file(profileName,"r")
            line = f.readline()
            while len(line):
                line = eval(line)
                profile = line
                line = f.readline()
                pass
            pass
        return (events,profile)
    
    def run_benchmark(self,working_path,template,dataset,propertyName,threads,txSize,txLimit):
        cwd = os.getcwd()
        os.chdir(working_path)
        jar = os.path.join(working_path,template,"build","benchmark.jar")
        arguments = ["java","-jar",jar,
                     "-op_path",dataset,
                     "-property",propertyName,
                     "-threads",str(threads),
                     "-tx_size",str(txSize),
                     "-tx_type","write",
                     "-tx_limit",str(txLimit),
                     "-ops","V",
                     "-no_map"
                     ]
        print string.join(arguments)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
        p.wait()
        os.chdir(cwd)
        (events,profile) = self.readProfileData(working_path)
        return (events,profile) 
    


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


    def setup_page_sizes(self,_page_size):
        page_size = []
        for i in _page_size:
            if i < 10:
                self.warn("Invalid page_size (%d) given must be between [10,16], setting to 10"%(i))
                i = 10
            elif i > 16:
                self.warn("Invalid page_size (%d) given must be between [10,16], setting to 16"%(i))
                i = 16
                pass
            page_size.append(pow(2,i))
            pass
        return page_size
    
    def is_runnable(self):
        return False

    def mkdir(self,name):
        try:
            if self.verbose > 1:
                print "Creating directory ",name
                pass
            if not os.path.exists(name):
                os.mkdir(name)
                pass
            return True
        except Exception,e:
            self.error(e)
            return False
        return True
    
    def output_string(self,string, status, bold):
        return _hilite_(string,status,bold)

    @classmethod
    def error(self,message):
        print >> sys.stderr,_hilite_("[Error]",False,True),_hilite_(message,False,False)
        return

    @classmethod
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
import build_operation
import bootstrap_operation
import dataset_operation
import vertex_ingest_operation
import report_operation

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
        add_operation(build_operation.operation())
        add_operation(bootstrap_operation.operation())
        add_operation(dataset_operation.operation())
        add_operation(vertex_ingest_operation.operation())
        add_operation(report_operation.operation())
        pass
    pass

def _operation_(name):
    populate()
    name = name.lower()
    if operations.has_key(name):
        return operations[name]
    return None



