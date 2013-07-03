import subprocess
import os
import threading
import time
import ig_property
import string
import sys
import bootstrap_operation
import build_operation
import Service
import types

class threaded_runner(threading.Thread):
    def __init__(self,parent,process):
        threading.Thread.__init__(self)
        self.parent = parent
        self.process = process
        self.events = None
        self.profile = None
        self.return_code = 1
        pass

    def run(self):
        self.operate()
        pass

    def operate(self):
        dataset = None
        profile_tag = "{0}.{1}".format(self.parent.profile_tag,self.getName())
        self.parent.operation.removeProfileData(self.parent.working_path,profile_tag)
        env = self.parent.operation.getEnv(self.parent.engine.version,self.parent.engine.home)
        if self.process == None:
            arguments = ["java","-Xmx5G","-jar",self.parent.jar,
                         "-property",self.parent.propertyFile.fileName,
                         "-threads",str(self.parent.threads),
                         "-tx_size",str(self.parent.tx_size),
                         "-tx_type",self.parent.tx_type,
                         "-tx_limit",str(self.parent.tx_limit),
                         "-profile",profile_tag,
                         ]
            if self.parent.operation.name == "vertex_ingest":
                dataset = self.parent.operation.GenerateDataset(self.parent.root_path,self.parent.template,self.parent.size,True)
                arguments.append("-op_path")
                arguments.append(dataset)
                arguments.append("-ops")
                arguments.append("V")
                arguments.append("-no_map")
            elif self.parent.operation.name == "edge_ingest":
                dataset = self.parent.operation.GenerateDataset(self.parent.root_path,self.parent.template,self.parent.size,False)
                arguments.append("-op_path")
                arguments.append(dataset)
                arguments.append("-ops")
                arguments.append("V,E")
            elif self.parent.operation.name == "query":
                dataset = self.parent.operation.GenerateQuery(self.parent.root_path,self.parent.template,self.parent.size,self.parent.graph_size,self.parent.vertex,self.parent.dist)
                if dataset == None:
                    return
                arguments.append("-op_file")
                arguments.append(dataset)
                arguments.append("-ops")
                arguments.append("Q")
                pass
            print "\t\t[{7}] Running benchmark (version:{0} config:{1} threads:{2} tx_size:{3} tx_type:{4} tx_limit:{5} process:{6})".format(self.parent.engine.version,
                                                                                                                                             self.parent.config,
                                                                                                                                             self.parent.threads,
                                                                                                                                             self.parent.tx_size,
                                                                                                                                             self.parent.tx_type,
                                                                                                                                             self.parent.tx_limit,
                                                                                                                                             self.parent.process,
                                                                                                                                             self.getName()
                                                                                                                                       ),self.parent.operation.name
            sys.stdout.flush()
            p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
            p.wait()
            self.return_code = p.returncode
            if p.returncode == 0:
                (self.events,self.profile) = self.parent.operation.readProfileData(self.parent.working_path,profile_tag)
                pass
            self.parent.operation.removeProfileData(self.parent.working_path,profile_tag)
            return True
        else:
            print self.process
            assert 0
        return False
    pass



class benchmark_runner:
    def __init__(self,_working_path,_root_path,_operation,
                 _new_graph,_version,_template,_config,
                 _size,_graph_size,
                 _page_size,_cache_size,_use_index,_threads,_tx_size,_tx_limit,
                 _tx_type,
                 _vertex=None,
                 _dist="uniform",
                 process=None
                 ):
        self.working_path = _working_path
        self.root_path = _root_path
        self.operation = _operation
        self.new_graph = _new_graph
        self.version = _version
        self.template = _template
        self.size = _size
        self.graph_size = _graph_size
        self.config = _config
        self.page_size = _page_size
        self.cache_size = _cache_size
        self.use_index = _use_index
        self.threads = _threads
        self.tx_size = _tx_size
        self.tx_limit = _tx_limit
        self.tx_type = _tx_type
        self.return_code = 1
        self.events = None
        self.profile = None
        self.vertex = _vertex
        self.dist = _dist
        self.process = process
        self.process_description = None
        self.number_processes = 1
        self.profile_sum = None
        self.profile_counter = 0
        pass

    def getProcessDescription(self):
        if self.process_description == None:
            return "local"
        return self.process_description 

    def message(self,counter,size):
        print "\t[{0}/{1}] Run {2}".format(counter,size,self.operation.name)
        pass

    def setup(self):
        configPair = self.operation.getConfigObject(self.root_path,self.config)
        if configPair == None:
            self.operation.error("Unable to get configuration {0}".format(self.config))
            return False
        (self.configList,self.configObject) = configPair
        if not self.configObject:
            self.operation.error("Unable to get Config object")
            return False
        self.engine = self.operation.getEngine(self.configList,self.version)
        if self.engine == None:
            self.operation.error("Unable to find configuration for InfiniteGraph version '{0}' using config {1}.".format(self.version,self.config))
            return False
        bootPath = None
        bootHost = None
        if len(self.configObject.hosts) > 0:
            if len(self.configObject.hosts[0].disks) > 0:
                p = os.path.join(self.configObject.hosts[0].disks[0].location,self.version,self.template)
                bootHost = self.configObject.hosts[0].address
                bootPath = p
                pass
            pass
        if self.new_graph:
            if Service.IsLocalAddress(bootHost):
                bootstrap = bootstrap_operation.operation()
                arguments = [
                    "--root","{0}".format(self.root_path),
                    "--config",self.config,
                    "--project",self.template,
                    "--page_size",self.page_size,
                    "--containers",1,
                    "--ig_version",self.version
                    ]
                if not self.use_index:
                    arguments.append("--no_index")
                    pass
                bootstrap.parse(arguments)
                if not bootstrap.operate():
                    self.operation.error("--Failed to bootstrap database.")
                    return False
                pass
            else:
                remoteRequest = Service.Request(bootHost)
                remoteRequest.init()
                remoteRequest.request("bootstrap",
                                      [
                                          "--root",".",
                                          "--config",self.config,
                                          "--project",self.template,
                                          "--page_size",self.page_size,
                                          "--containers",1,
                                          "--ig_version",self.version
                                          ]
                                      )
                remoteRequest.run()
                print remoteRequest.response
                pass
            pass
        self.profile_tag = str(int(round(time.time() * 1000)))
        project_path = os.path.join(self.working_path,self.template)
        if not os.path.exists(project_path):
            self.operation.warn("Project path '{0}' does not exist. Trying to build project.".format(self.template))
            build = build_operation.operation()
            build.parse(["--root","{0}".format(self.root_path),
                         "--ig_home","{0}".format(self.engine.home),
                         "--ig_version","{0}".format(self.engine.version)])
            build.operate()
            if not os.path.exists(project_path):
                self.operation.error("Project path '{0}' does not exist after an attempted build.".format(self.template))
                return False
            pass
        self.propertyFile = ig_property.PropertyFile(os.path.join(project_path,"properties","{0}.properties".format(self.profile_tag)))
        self.propertyFile.setLockServer(self.configObject.lockserver)
        self.propertyFile.setBootPath("{0}::{1}".format(bootHost,bootPath))
        self.propertyFile.properties["IG.MasterDatabaseHost"] = bootHost
        self.propertyFile.properties["IG.MasterPath"] = bootPath
        self.propertyFile.generate()
        self.propertyFile.setPageSize(pow(2,self.page_size))
        self.jar = os.path.join(self.working_path,self.template,"build","benchmark.jar")
        return True

    def add_profile(self,profile):
        if profile:
            for i in profile:
                print "\t\t\tProcess profile:",i["data"]
            if self.profile_sum == None:
                self.profile_counter = 1
                self.profile_sum = profile
            else:
                if len(self.profile_sum) != len(profile):
                    print "Inconsistent profile data, Ignoring it (length does not match)."
                    print self.profile_sum
                    print profile
                    self.return_code = 1
                else:
                    self.profile_counter += 1
                    for i in xrange(len(self.profile_sum)):
                        keys_sum = self.profile_sum[i]["data"].keys()
                        keys_profile = profile[i]["data"].keys()
                        if keys_sum == keys_profile:
                            self.profile_sum[i]["numberOfThreads"] += profile[i]["numberOfThreads"]
                            self.profile_sum[i]["memMax"] += profile[i]["memMax"]
                            self.profile_sum[i]["memInit"] += profile[i]["memInit"]
                            self.profile_sum[i]["memCommitted"] += profile[i]["memCommitted"]
                            self.profile_sum[i]["time"] += profile[i]["time"]
                            self.profile_sum[i]["txsize"] += profile[i]["txsize"]
                            self.profile_sum[i]["memUsed"] += profile[i]["memUsed"]
                            for j in keys_sum:
                                self.profile_sum[i]["data"][j] += profile[i]["data"][j]
                            pass
                        else:
                            print "Inconsistent profile data, Ignoring it. {0} {1} {2}".format(keys_sum == keys_profile,keys_sum,keys_profile)
                            self.return_code = 1
                            pass
                        pass
                    pass
                pass
            pass
        pass

    def average_profile(self):
        if self.profile_sum:
            for i in xrange(len(self.profile_sum)):
                keys_sum = self.profile_sum[i]["data"].keys()
                self.profile_sum[i]["numberOfThreads"] = int(self.profile_sum[i]["numberOfThreads"]/self.profile_counter)
                self.profile_sum[i]["memMax"] = (self.profile_sum[i]["memMax"]/self.profile_counter)
                self.profile_sum[i]["memInit"] = (self.profile_sum[i]["memInit"]/self.profile_counter)
                self.profile_sum[i]["memCommitted"] = (self.profile_sum[i]["memCommitted"]/self.profile_counter)
                self.profile_sum[i]["time"] = (self.profile_sum[i]["time"]/self.profile_counter)
                self.profile_sum[i]["txsize"] = int(self.profile_sum[i]["txsize"]/self.profile_counter)
                self.profile_sum[i]["memUsed"] = int(self.profile_sum[i]["memUsed"]/self.profile_counter)
                for j in keys_sum:
                    self.profile_sum[i]["data"][j] = self.profile_sum[i]["data"][j] 
                    pass
                pass
            pass
        self.profile = self.profile_sum
        pass
    
    def operate(self):
        if self.process == None:
            self.process = (None,1)
            pass
        elif type(self.process) != types.TupleType:
            self.operation.error("Invalid process argument {0} must be a tuple (host,numberOfProcesses)".format(self.process))
            return False
        elif len(self.process) == 1:
            self.process = (self.process[0],1)
        elif len(self.process) != 2:
            self.operation.error("Invalid process argument {0} must be a tuple of size 2 (host,numberOfProcesses)".format(self.process))
            return False
        self.process_description = self.process[0]
        self.number_processes = self.process[1]
        if type(self.number_processes) != types.IntType:
            self.operation.error("Number of processes must be an integer. given:{0} type:{1}".format(self.number_processes,type(self.number_processes))) 
            return False
        runners = []
        cwd = os.getcwd()
        os.chdir(self.working_path)
        for i in xrange(self.number_processes):
            runner = threaded_runner(self,self.process_description)
            runners.append(runner)
            pass
        for i in runners:
            i.start()
            pass
        for i in runners:
            i.join()
            pass
        self.return_code = -1
        for i in runners:
            if self.return_code == -1:
                self.return_code = 0
                pass
            self.return_code += i.return_code
            if i.events != None:
                if self.events == None:
                    self.events = i.events
                else:
                    self.events = self.events + i.events
                    pass
                pass
            pass
        for i in runners:
            if i.profile != None:
                self.add_profile(i.profile)
                pass
            pass
        self.average_profile()
        if self.profile:
            for i in self.profile:
                print "\t\t\tOverall profile:",i["data"]
                pass
            pass
        os.chdir(cwd)
        return True
    pass

        
