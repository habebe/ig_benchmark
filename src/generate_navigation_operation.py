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

class operation(operations.operation):
    "Build a navigation dataset"
    def __init__(self):
        operations.operation.__init__(self,"generate_navigate",False)
        self.add_argument("help",None,None,"show help message")
        self.add_argument("root","str",None,"root path")
        self.add_argument("template","str",None,"template name")
        self.add_argument("size","int",10000,"verbose level")
        self.add_argument("source","str",".","source path")
        self.add_argument("target","str","query","target path")
        self.add_argument("vertex","str",None,"vertex name")
        self.add_argument("dist","str","uniform","query distribution: uniform , gauss,mu,sigma")
        self.add_argument("verbose","int",0,"verbose level")
        self.add_argument("dfs",None,None,"Use Depth first search")
        self.add_argument("depth","int","5","Navigation Depth")
        self.add_argument("results","int","100","Max number of results.")
        pass

    def __run__(self,template,source,target,size,verbose,dist,vertex):
        ig_template.Data.Options.source = source
        ig_template.Data.Options.target_file = target
        parser = ig_template.Schema.Parser(template,None)
        if dist == "uniform":
            parser.setUniformDistribution()
        elif dist == "gauss":
            if len(dist) != 3:
                self.error("When using gauss distribution, you must specify gauss,mu,sigma")
                return False
            if (float(dist[1]) < 0) or (float(dist[1]) > 1) or (float(dist[2]) < 0) or (float(dist[2]) > 1):
                self.error("When using gauss distribution, values of mu,sigma must be between 0 and 1.")
                return False
            parser.setGaussDistribution(float(dist[1]),float(dist[2]))
            pass
        parser.setVerboseLevel(verbose)
        parser.generateQuery(vertex,size)
        ig_template.Data.Options.CloseAllFiles()
        return True
        
    def operate(self):
        if operations.operation.operate(self):
            rootPath = self.getSingleOption("root")
            template = self.getSingleOption("template")
            size = self.getSingleOption("size")
            source = self.getSingleOption("source")
            target = self.getSingleOption("target")
            dist = self.getSingleOption("dist")
            vertex = self.getOption("vertex")
            if rootPath == None:
                self.error("Root path is not given.")
                return False
            if template == None:
                self.error("Template is not given.")
                return False
            if not template.endswith(".xml"):
                template = template + ".xml"
                pass
            if (not vertex) or len(vertex) == 0:
                self.error("Vertex is not given.")
                return False
            template = os.path.join(rootPath,"templates",template)
            if not os.path.exists(template):
                self.error("Unable to find template file {0}.".format(template))
                return False
            return self.__run__(template,source,target,size,self.verbose,dist,vertex)
        return True
