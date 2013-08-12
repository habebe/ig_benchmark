import os
import sys
import getopt
import operations
import shutil
from db import *
import ig_template
import os
import subprocess
import string
import AgentHandler
import build_operation

class operation(operations.operation):
    "Start agent manager"
    Manager = None
    def __init__(self):
        operations.operation.__init__(self,"agents",False)
        self.add_argument("help",None,None,"show help message")
        self.add_argument("ig_version","str",None,"InfiniteGraph version.")
        self.add_argument("config","str",None,"specify the config file name and the name of the config using the format config_file_name:config_name.")
        self.add_argument("root","str",".","root path")
        self.add_argument("template","str",None,"template name")
        self.add_argument("project","str",None,"project name")
        self.add_argument("name","str","IngestAgent","agents name")
        self.add_argument("bootfile","str",None,"bootfile")
        self.add_argument("property","str",None,"property File name")
        self.add_argument("num_agents","int","1","number of agents to start")
        self.add_argument("poll_time","int","3","poll time.")
        self.add_argument("tc_timeout","float","-1","Terminate condition:Time in seconds to wait until the agents are killed -1=forever")
        self.add_argument("tc_process","float","-1","Terminate condition:Number of tasks to be processed until the agents are killed -1=forver")
        self.add_argument("wait","int","1","wait for agent to complete")
        self.add_argument("verbose","int",0,"verbose level")
        pass

    @classmethod
    def StartAgents(self,engine,rootPath,template,name,bootfile,property,num_agents,poll_time,
                    timeout,processed,userTaskPath,verbose,logging_properties,wait,env):
        if self.Manager != None:
            self.error("There is an existing manager running, unable to start anaother one.")
            return False
       
        self.Manager = AgentHandler.AgentManager(name,
                                                 engine.home,
                                                 bootfile,
                                                 property,
                                                 num_agents,
                                                 poll_time,
                                                 AgentHandler.UpsertAgentLogParser,
                                                 AgentHandler.AgentTerminateCondition(timeout,processed),
                                                 userTaskPath,
                                                 logging_properties,
                                                 env
                                                 )
        self.Manager.start()
        if wait:
            self.Manager.join()
            totalTime = 0
            totalItems = 0
            for i in self.Manager.agents:
                counter  = i.logParser.processCounter
                if counter:
                    timeDiff = i.logParser.preProcessTimeStamp[1] - i.logParser.preProcessTimeStamp[0]
                    totalTime += timeDiff
                    totalItems += counter
                    print i.agentName,"time :",timeDiff," Rate:",(counter*1.0/timeDiff)," Counter:",counter
                    pass
                pass
            self.Manager = None
            print "Total time:",totalTime," counter:",totalItems
            pass
        return True


    @classmethod
    def StopAgents(self,wait):
        if self.Manager == None:
            self.error("Agent manager thread is not running.")
            return False
        self.Manager.terminateAgents()
        if wait:
            self.Manager.join()
            totalTime = 0
            totalItems = 0
            for i in self.Manager.agents:
                counter  = i.logParser.processCounter
                if counter:
                    timeDiff = i.logParser.preProcessTimeStamp[1] - i.logParser.preProcessTimeStamp[0]
                    totalTime += timeDiff
                    totalItems += counter
                    print i.agentName,"time :",timeDiff," Rate:",(counter*1.0/timeDiff)," Counter:",counter
                    pass
                pass
            self.Manager = None
            print "Total time:",totalTime," counter:",totalItems
            pass
        return True
        
    def operate(self):
        if operations.operation.operate(self):
            ig_version = self.getSingleOption("ig_version")
            configParameter = self.getSingleOption("config")
            rootPath = self.getSingleOption("root")
            template = self.getSingleOption("template")
            project = self.getSingleOption("project")
            name = self.getSingleOption("name")
            bootfile = self.getSingleOption("bootfile")
            property = self.getSingleOption("property")
            num_agents = self.getSingleOption("num_agents")
            poll_time = self.getSingleOption("poll_time")
            timeout = self.getSingleOption("tc_timeout")
            processed = self.getSingleOption("tc_process")
            verbose = self.getSingleOption("verbose")
            wait = self.getSingleOption("wait")
            logging_properties = os.path.join(os.path.dirname(__file__),"properties","task.logging.properties")
            
            if ig_version == None:
                self.error("InfiniteGraph version is not given.")
                return False
            if rootPath == None:
                self.error("Root path is not given.")
                return False
            rootPath = os.path.abspath(rootPath)
            if project == None:
                self.error("Project is not given.")
                return False
            
            if not configParameter:
                self.error("Config Parameter is not given.")
                return False
            configPair = self.getConfigObject(rootPath,configParameter)
            if configPair == None:
                self.error("Unable to get configuration {0}".format(configParameter))
                return False
            (configList,configObject) = configPair
            if configObject == None:
                self.error("Unable to get configuration object {0}".format(configParameter))
                return False
            engine = self.getEngine(configList,ig_version)
            if engine == None:
                self.error("Unable to find configuration for InfiniteGraph version '{0}' using config {1}.".format(ig_version,configParameter))
                return False
            
            working_path = self.setupWorkingPath(rootPath,engine.version)
            if template == None:
                self.error("Template is not given.")
                return False
            if not template.endswith(".xml"):
                template = template + ".xml"
                pass
            template = os.path.join(rootPath,"templates",template)
            if not os.path.exists(template):
                self.error("Unable to find template file {0}.".format(template))
                return False
            
            if bootfile == None:
                self.error("Boot file is not given.")
                return False

            project_path = os.path.join(working_path,project)
            build = build_operation.operation()
            build.parse(["--root","{0}".format(rootPath),
                         "--ig_home","{0}".format(engine.home),
                         "--has_tasks",
                         "--ig_version","{0}".format(engine.version)])
            build.operate()
            if not os.path.exists(project_path):
                self.error("Project path '{0}' does not exist after an attempted build.".format(project))
                return False
            userTaskPath = os.path.join(project_path,"pipeline")
            return self.StartAgents(engine,rootPath,template,name,bootfile,property,num_agents,poll_time,
                                    timeout,processed,userTaskPath,verbose,logging_properties,wait,
                                    self.getEnv(engine.version,engine.home)
                                    )
        return True
