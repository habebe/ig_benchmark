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
import threading

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
    return string
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


    GenerateDataset_Semaphore = threading.Semaphore()
    @classmethod
    def GenerateDataset(self,rootPath,template,size,vertexOnly):
        self.GenerateDataset_Semaphore.acquire()
        dataPath = os.path.join(rootPath,"working","_dataset_")
        if not os.path.exists(dataPath):
            os.mkdir(dataPath)
            pass
        dataPath = os.path.join(dataPath,"{0}.{1}.{2}".format(template,size,vertexOnly))
        generate = (os.path.exists(dataPath) == False)
        if not generate:
            if len(os.listdir(dataPath)) == 0:
                print "Warning bad data path"
                generate = True
                pass
            pass
        if generate:
            if not os.path.exists(dataPath):
                os.mkdir(dataPath)
                pass
            print "\t\tGenerating dataset template:{0} size:{1} path:{2}".format(template,size,dataPath),
            sys.stdout.flush()
            import dataset_operation
            dataset = dataset_operation.operation()
            sourcePath = os.path.join(rootPath,"data_source")
            if vertexOnly:
                dataset.parse([
                    "--root","{0}".format(rootPath),
                    "--template",template,
                    "--size",size,
                    "--source",sourcePath,
                    "--target",dataPath,
                    "--vertex_only"
                    ])
            else:
                dataset.parse([
                    "--root","{0}".format(rootPath),
                    "--template",template,
                    "--size",size,
                    "--source",sourcePath,
                    "--target",dataPath,
                    ])
                pass
            dataset.operate()
            print "[complete]"
        else:
            print "\t\tReusing previously generated dataset template:{0} size:{1} path:{2}".format(template,size,dataPath)
            pass
        self.GenerateDataset_Semaphore.release()
        return dataPath

    GenerateNav_Semaphore = threading.Semaphore()
    @classmethod
    def GenerateNavigation(self,rootPath,template,size,graph_size,vertex,dist="uniform"):
        self.GenerateNav_Semaphore.acquire()
        dataPath = os.path.join(rootPath,"working","_dataset_")
        if not os.path.exists(dataPath):
            os.mkdir(dataPath)
            pass
        sourcePath = os.path.join(dataPath,"{0}.{1}.{2}".format(template,graph_size,True))
        if not os.path.exists(sourcePath):
            sourcePath = os.path.join(dataPath,"{0}.{1}.{2}".format(template,graph_size,False))
            pass
        if not os.path.exists(sourcePath):
            self.error("Unable to find source path '{0}' when generating queries.".format(sourcePath))
            self.GenerateNav_Semaphore.release()
            return None
        dataPath = os.path.join(dataPath,"query.{0}.{1}.{2}.{3}".format(template,vertex,graph_size,size))
        if not os.path.exists(dataPath):
            print "\t\tGenerating template template:{0} size:{1} path:{2}".format(template,size,dataPath),
            sys.stdout.flush()
            import generate_query_operation
            dataset = generate_query_operation.operation()
            dataset.parse([
                "--root","{0}".format(rootPath),
                "--template",template,
                "--size",size,
                "--source",sourcePath,
                "--target",dataPath,
                "--vertex",vertex,
                "--dist",dist
                ])
            dataset.operate()
            print "[complete]"
        else:
            print "\t\tReusing previously generated query template:{0} size:{1} path:{2}".format(template,size,dataPath)
            pass
        self.GenerateNav_Semaphore.release()
        return dataPath

    GenerateQuery_Semaphore = threading.Semaphore()
    @classmethod
    def GenerateQuery(self,rootPath,template,size,graph_size,vertex,dist="uniform"):
        self.GenerateQuery_Semaphore.acquire()
        dataPath = os.path.join(rootPath,"working","_dataset_")
        if not os.path.exists(dataPath):
            os.mkdir(dataPath)
            pass
        sourcePath = os.path.join(dataPath,"{0}.{1}.{2}".format(template,graph_size,True))
        if not os.path.exists(sourcePath):
            sourcePath = os.path.join(dataPath,"{0}.{1}.{2}".format(template,graph_size,False))
            pass
        if not os.path.exists(sourcePath):
            self.error("Unable to find source path '{0}' when generating queries.".format(sourcePath))
            return None
        dataPath = os.path.join(dataPath,"query.{0}.{1}.{2}.{3}".format(template,vertex,graph_size,size))
        if not os.path.exists(dataPath):
            print "\t\tGenerating template template:{0} size:{1} path:{2}".format(template,size,dataPath),
            sys.stdout.flush()
            import generate_query_operation
            dataset = generate_query_operation.operation()
            dataset.parse([
                "--root","{0}".format(rootPath),
                "--template",template,
                "--size",size,
                "--source",sourcePath,
                "--target",dataPath,
                "--vertex",vertex,
                "--dist",dist
                ])
            dataset.operate()
            print "[complete]"
        else:
            print "\t\tReusing previously generated query template:{0} size:{1} path:{2}".format(template,size,dataPath)
            pass
        self.GenerateQuery_Semaphore.release()
        return dataPath
    
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
        try:
            configList = config.parse(configFileName,self)
        except:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            self.error("Error while parsing config file. {0}".format(exc_obj))
            return None
        return (configList,configName)

    def getEnv(self,ig_version,ig_home):
        env = os.environ.copy()
        platform_name = platform.system().lower()
        if platform_name.find("darwin") >= 0:
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
            pass
        elif platform_name.find("linux") >= 0:
            env["IG_HOME"] = os.path.join(ig_home,"linux86_64")
            if env.has_key("LD_LIBRARY_PATH"):
                env["LD_LIBRARY_PATH"] = "{0}:{1}".format(os.path.join(env["IG_HOME"],"lib"),env["LD_LIBRARY_PATH"])
            else:
                env["LD_LIBRARY_PATH"] = os.path.join(env["IG_HOME"],"lib")
                pass
            if env.has_key("PATH"):
                env["PATH"] = "{0}:{1}".format(os.path.join(env["IG_HOME"],"bin"),env["PATH"])
            else:
                env["PATH"] = os.path.join(env["IG_HOME"],"bin")
                pass
            pass
        else:
            assert 0,"Unknown platform."
            pass
        return env
    
    def setupWorkingPath(self,rootPath,version):
        working_path = os.path.join(rootPath,"working")
        data_path = os.path.join(rootPath,"working","data")
        version_path = os.path.join(rootPath,"working",version)
        self.mkdir(working_path)
        self.mkdir(data_path)
        self.mkdir(version_path)
        return version_path
     
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
    
    def removeProfileData(self,working_path,tag):
        events  = os.path.join(working_path,"{0}.benchmark.events".format(tag))
        profile = os.path.join(working_path,"{0}.benchmark.profile".format(tag))
        if os.path.exists(events):
            os.remove(events)
            pass
        if os.path.exists(profile):
            os.remove(profile)
            pass
        pass

    def readProfileData(self,working_path,tag):
        eventsName  = os.path.join(working_path,"{0}.benchmark.events".format(tag))
        profileName = os.path.join(working_path,"{0}.benchmark.profile".format(tag))
        events  = []
        profile = []
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
                profile.append(line)
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
        self.db = None
        if name == None:
            self.name = self.get_name()
        else:
            self.name = name
            pass
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

    def remove_argument(self,name):
        if self.argumentMap.has_key(name):
            self.argumentMap.pop(name)
            counter = 0
            done = (len(self.argumentDescription) == 0)
            while not done:
                o = self.argumentDescription[counter]
                if o.name == name:
                    self.argumentDescription.pop(counter)
                    done = True
                    pass
                else:
                    done = (counter >= len(self.argumentDescription))
                    pass
                counter += 1
                pass
            pass
        pass
    
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
        print >> sys.stderr,_hilite_("\t\t[Error]",False,True),_hilite_(message,False,False)
        return

    @classmethod
    def warn(self,message):
        print >> sys.stderr,_hilite_("\t\t[Warning]",False,True),_hilite_(message,False,False)
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
            self.optionsMap.clear()
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
        self.db_name = self.getSingleOption("name")
        return 1
    pass


operations = {}
import run_operation
import init_operation
import build_operation
import bootstrap_operation
import dataset_operation
import vertex_ingest_operation
import edge_ingest_operation
import pipeline_edge_ingest_operation
import query_operation
import report_operation
import generate_query_operation
import merge_operation
import service_operation
import mkdir_operation
    
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
        add_operation(generate_query_operation.operation())
        add_operation(vertex_ingest_operation.operation())
        add_operation(edge_ingest_operation.operation())
        add_operation(pipeline_edge_ingest_operation.operation())
        add_operation(query_operation.operation())
        add_operation(report_operation.operation())
        add_operation(merge_operation.operation())
        add_operation(service_operation.operation())
        add_operation(mkdir_operation.operation())
        pass
    pass

def _operation_(name):
    populate()
    name = name.lower()
    if operations.has_key(name):
        return operations[name]
    return None



