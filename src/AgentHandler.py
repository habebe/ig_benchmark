import threading
import subprocess
import sys
import time
import os
import socket
import string

class AgentLogParser:
    def __init__(self):
        pass

    def parseMessage(self,line):
        end = line.find("]")
        if end != -1:
            start = line.find("[")
            if start != -1:
                timestamp = line[start+1:end].strip()
                message   = line[end:]
                message   = message[message.find(":")+1:].strip()
                return (timestamp,message)
            pass
        return None
    pass

class UpsertAgentLogParser(AgentLogParser):
    def __init__(self):
        self.preProcessTimeStamp = [None,None]
        self.processTimeStamp    = [None,None]
        self.preProcessCounter = 0
        self.processCounter = 0
        pass

    def parse(self,line):
        data = self.parseMessage(line)
        if data != None:
            message = data[1]
            data = message.split(",")
            if (data != None) and len(data) >= 4:
                T = data[0].strip()
                if T == "U":
                    type = int(data[1])
                    timestamp = float(data[2])*0.001
                    counter = float(data[3])
                    if type == 0:
                        if self.preProcessTimeStamp[0] == None:
                            self.preProcessTimeStamp[0] = timestamp
                            pass
                        self.preProcessTimeStamp[1] = timestamp
                        self.preProcessCounter = counter
                    elif type == 1:
                        if self.processTimeStamp[0] == None:
                            self.processTimeStamp[0] = timestamp
                            pass
                        self.processTimeStamp[1] = timestamp
                        self.processCounter = counter
                        pass
                    return True
                    pass
                pass
            pass
        return False
    pass
    
class AgentHandler(threading.Thread):
    def __init__(self,manager,monitor):
        threading.Thread.__init__(self)
        self.manager = manager
        self.agentName = "{0}.{1}".format(self.manager.name,self.getName())
        self.stdoutName = "{0}.stdout".format(self.agentName)
        self.stderrName = "{0}.stderr".format(self.agentName)
        self.logParser = self.manager.logParserType()
        self.process = None
        self.arguments = None
        self.processStatus = None
        self.shouldTerminate = False
        self.currentStdoutLocation = None
        self.stdoutFile = None
        self.stderrFile = None
        self.monitor = monitor
        pass

    def getLibDir(self):
        return os.path.join(self.manager.IG_HOME,"lib")

    def getEtcDir(self):
        return os.path.join(self.manager.IG_HOME,"etc")

    def getIGJar(self):
        return os.path.join(self.getLibDir(),"InfiniteGraph.jar")

    def getLogJar(self):
        return os.path.join(self.getLibDir(),"slf4j-jdk14-1.6.1.jar")

    def getConfigFile(self):
        return os.path.join(self.getEtcDir(),"PipelineAgentPrefs.config")

    def setup(self):
        if self.manager.IG_HOME == None:
            print "IG_HOME is None"
            return False
        if not os.path.exists(self.manager.IG_HOME):
            print "IG_HOME={0} does not exist.".format(self.manager.IG_HOME)
            return False
        if not os.path.exists(self.getIGJar()):
            print "IG jar does not exist",self.getIGJar()
            return False
        if not os.path.exists(self.getLogJar()):
            print "Log jar does not exist",self.getLogJar()
            return False
        if not os.path.exists(self.getConfigFile()):
            print "Config file",self.getConfigFile(),"does not exist"
            return False
        return True
        
    def terminate(self):
        self.shouldTerminate = True
        pass

    def __run_agent__(self):
        self.shouldTerminte = False
        stdoutFile = file(self.stdoutName,"w")
        stderrFile = file(self.stderrName,"w")
        if self.manager.env:
            self.process = subprocess.Popen(self.arguments,stdout=stderrFile,stderr=stdoutFile,env=self.manager.env)
        else:
            self.process = subprocess.Popen(self.arguments,stdout=stderrFile,stderr=stdoutFile)
            pass
        self.manager.output_message("Started agent pid:{0} log:{1}".format(self.process.pid,self.stdoutName))
        pass

    def readStdoutLines(self):
        lines = []
        if self.currentStdoutLocation != None:
            self.stdoutFile.seek(self.currentStdoutLocation)
            pass
        line = self.stdoutFile.readline()
        while len(line):
            if line[len(line)-1] == "\n":
                line = line.strip()
                lines.append(line)
                self.currentStdoutLocation = self.stdoutFile.tell()
                pass
            line = self.stdoutFile.readline()
            pass
        return lines

    def processLog(self):
        if self.stdoutFile == None:
            self.stdoutFile = file(self.stdoutName,"r")
            pass
        lines = self.readStdoutLines()
        for line in lines:
            self.logParser.parse(line)
            pass
        pass

    def processErrorLog(self):
        stderrFile = file(self.stdoutName,"r")
        line = stderrFile.readline()
        while len(line):
            self.manager.output_message("{0} error:{1}".format(self.getName(),line))
            line = stderrFile.readline()
            pass
        pass
    
    def run(self):
        self.arguments = ["java","-cp"]
        self.arguments.append("{0}:{1}".format(self.getLogJar(),self.getIGJar()))
        self.arguments.append("com.infinitegraph.impl.plugins.pwp.pipelining.PipelineAgent")
        self.arguments.append("-configFile")
        self.arguments.append(self.getConfigFile())
        if self.monitor:
            self.arguments.append("-monitor")
            pass
        self.arguments.append("-bootfile")
        self.arguments.append(self.manager.bootFile)
        self.arguments.append("-userTaskDirectory")
        self.arguments.append(self.manager.userTaskDirectory)
        self.arguments.append("-loggingProperties")
        self.arguments.append(self.manager.loggingProperties)
        #self.arguments = ["python","dummy.py"] 
        self.manager.output_message("{0} Agent arguments:{1}".format(self.getName(),string.join(self.arguments)))
        self.__run_agent__()
        done = False
        while not done:
            time.sleep(self.manager.pollTime)
            self.processStatus = self.process.poll()
            self.processLog()
            if self.processStatus != None:
                done = True
            elif self.shouldTerminate:
                self.manager.output_message("Terminating Agent pid:{0}.".format(self.process.pid))
                self.process.kill()
                done = True
                pass
            pass
        self.processLog()
        #self.processErrorLog()
        self.manager.output_message("Agent Process Status:{0} pid:{1}".format(self.processStatus,self.process.pid))
        self.process = None
        pass
    pass


class AgentTerminateCondition:
    def __init__(self,timeout,numberOfProcessed):
        self.timeout = timeout
        self.numberOfProcessed = numberOfProcessed
        self.currentTimeoutCounter = 0
        pass

    def shouldTerminate(self,numberOfProcessed):
        status = False
        self.currentTimeoutCounter += 1
        if self.timeout > 0:
            if self.currentTimeoutCounter >= self.timeout:
                status = True
                print "[AgentManager] Timeout condition reached counter:{0} limit:{1}".format(self.currentTimeoutCounter,self.timeout)
                pass
            pass
        if self.numberOfProcessed > 0:
            if self.numberOfProcessed <= numberOfProcessed:
                status = True
                print "[AgentManager] Max processed condition reached counter:{0} limit:{1}".format(numberOfProcessed,self.numberOfProcessed)
                pass
            pass
        if status == False:
            print "[AgentManager] {0} Terminate condition not reached processed:{1}.".format(self.currentTimeoutCounter,numberOfProcessed)
            pass
        return status
    pass

class AgentManager(threading.Thread):
    def __init__(self,name,IG_HOME,bootFile,propertyFile,numberOfAgents,
                 pollTime,logParserType,terminateCondition,
                 userTaskDirectory,loggingProperties,
                 env):
        threading.Thread.__init__(self)
        self.name = "{0}.{1}".format(name,int(time.time()*1000))
        self.IG_HOME = IG_HOME
        self.bootFile = bootFile
        self.propertyFile = propertyFile
        self.numberOfAgents = numberOfAgents
        self.pollTime = pollTime
        self.logParserType = logParserType
        self.terminateCondition = terminateCondition
        self.userTaskDirectory = userTaskDirectory
        self.loggingProperties = loggingProperties
        self.agents = []
        self.numberOfTotalProcessed = 0
        self.shouldTerminate = False
        self.env = env
        self.messageList = []
        pass

    outputLock = threading.Lock()
    def print_messages(self):
        self.outputLock.acquire()
        for message in self.messageList:
            print "[AgentManager] {0}".format(message)
            pass
        self.messageList = []
        self.outputLock.release()
        pass
    
    def output_message(self,message):
        self.outputLock.acquire()
        self.messageList.append(message)
        self.outputLock.release()
        pass
    

    def startAgents(self):
        self.shouldTerminate = False
        monitor = True
        for i in xrange(self.numberOfAgents):
            agent = AgentHandler(self,monitor)
            if agent.setup() == True:
                self.agents.append(agent)
                #monitor = False
                pass
            pass
        for i in self.agents:
            i.start()
            pass
        pass

    def terminateAgents(self):
        for i in self.agents:
            i.terminate()
            pass
        self.shouldTerminate = True
        pass

    def joinAgents(self):
        for i in self.agents:
            i.join()
            pass
        pass

    def numberOfActiveAgents(self):
        return len(self.agents)

    def terminateConditionReached(self,numberOfTotalProcessed):
        if self.shouldTerminate:
            return True
        return self.terminateCondition.shouldTerminate(numberOfTotalProcessed)
    
    def run(self):
        self.startAgents()
        done = (self.numberOfActiveAgents() == 0)
        while not done:
            self.print_messages()
            time.sleep(self.pollTime)
            self.numberOfTotalProcessed = 0
            for agent in self.agents:
                self.numberOfTotalProcessed += int(agent.logParser.processCounter)
                pass
            done = self.terminateConditionReached(self.numberOfTotalProcessed)
            if not done:
                dead = 0
                for i in self.agents:
                    if (i.process == None):
                        dead += 1
                        pass
                    pass
                done = (dead == len(self.agents))
                if done:
                    self.output_message("All agents process are dead.")
                    pass
                pass
            pass
        self.terminateAgents()
        self.print_messages()
        pass

    def join(self):
        self.joinAgents()
        threading.Thread.join(self)
        self.print_messages()
        sys.stdout.flush()
        return
    pass


if 1:
    manager = AgentManager('CompositeIngest',
                           '/Applications/InfiniteGraph/3.1.task/',
                           '127.0.0.1::/Users/henocka/cisco/WORK_AREA/whois_new/data/whois.boot',
                           'propertyFile',
                           2,
                           2.0,
                           UpsertAgentLogParser,
                           AgentTerminateCondition(10,10),
                           "./pipeline/",
                           "./properties/logging.properties",
                           None
                           )
    manager.start()
    manager.join()
    totalTime = 0
    totalItems = 0
    for i in manager.agents:
        counter  = i.logParser.processCounter
        if counter:
            timeDiff = i.logParser.preProcessTimeStamp[1] - i.logParser.preProcessTimeStamp[0]
            totalTime += timeDiff
            totalItems += counter
            print i.agentName,"time :",timeDiff," Rate:",(counter*1.0/timeDiff)," Counter:",counter
            pass
        pass
    print "Total time:",totalTime," counter:",totalItems
    
