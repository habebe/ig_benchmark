import subprocess
import os
import threading
import time
import ig_property
import string
import sys
import bootstrap_operation

class benchmark_runner(threading.Thread):
    def __init__(self,_working_path,_root_path,_operation,
                 _new_graph,_version,_template,_config,
                 _size,_graph_size,
                 _page_size,_cache_size,_use_index,_threads,_tx_size,_tx_limit,
                 _tx_type,
                 _vertex=None,_dist="uniform"
                 ):
        threading.Thread.__init__(self)
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
        pass

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
        if len(self.configObject.hosts) > 0:
            if len(self.configObject.hosts[0].disks) > 0:
                p = os.path.join(self.configObject.hosts[0].disks[0].location,self.version,self.template)
                #print self.operation.output_string("Bootpath:{0}".format(p),True,True)
                bootPath = p
                pass
            pass
        if self.new_graph:
            #print "Create new graph"
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
        self.profile_tag = str(int(round(time.time() * 1000)))
        project_path = os.path.join(self.working_path,self.template)
        self.propertyFile = ig_property.PropertyFile(os.path.join(project_path,"properties","{0}.properties".format(self.profile_tag)))
        #print self.operation.output_string("Generating ingest propertyFile {0}".format(self.propertyFile.fileName),True,False)
        self.propertyFile.setLockServer(self.configObject.lockserver)
        self.propertyFile.setBootPath(bootPath)
        self.propertyFile.generate()
        self.propertyFile.setPageSize(pow(2,self.page_size))
        self.jar = os.path.join(self.working_path,self.template,"build","benchmark.jar")
        return True

    def operate(self):
        cwd = os.getcwd()
        os.chdir(self.working_path)
        dataset = None
        self.operation.removeProfileData(self.working_path,self.profile_tag)
        env = self.operation.getEnv(self.engine.version,self.engine.home)
        arguments = ["java","-jar",self.jar,
                     "-property",self.propertyFile.fileName,
                     "-threads",str(self.threads),
                     "-tx_size",str(self.tx_size),
                     "-tx_type",self.tx_type,
                     "-tx_limit",str(self.tx_limit),
                     "-profile",self.profile_tag,
                     ]

        if self.operation.name == "vertex_ingest":
            dataset = self.operation.GenerateDataset(self.root_path,self.template,self.size,True)
            arguments.append("-op_path")
            arguments.append(dataset)
            arguments.append("-ops")
            arguments.append("V")
            arguments.append("-no_map")
        elif self.operation.name == "query":
            dataset = self.operation.GenerateQuery(self.root_path,self.template,self.size,self.graph_size,self.vertex,self.dist)
            if dataset == None:
                return
            arguments.append("-op_file")
            arguments.append(dataset)
            arguments.append("-ops")
            arguments.append("Q")
            pass

        #print string.join(arguments)
        print "\t\tRunning benchmark (threads:{0} tx_size:{1} tx_type:{2} tx_limit:{3})".format(self.threads,
                                                                                                self.tx_size,
                                                                                                self.tx_type,
                                                                                                self.tx_limit
                                                                                                ),self.operation.name,
        sys.stdout.flush()
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr,env=env)
        p.wait()
        if p.returncode == 0:
            print "[complete]"
        else:
            print "[failed]"
            pass
        self.return_code = p.returncode
        if p.returncode == 0:
            (self.events,self.profile) = self.operation.readProfileData(self.working_path,self.profile_tag)
            pass
        self.operation.removeProfileData(self.working_path,self.profile_tag)
        os.chdir(cwd)
        pass
    pass

        
