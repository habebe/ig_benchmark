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
    "Build a template"
    def __init__(self):
        operations.operation.__init__(self,"dataset",False)
        self.add_argument("help",None,None,"show help message")
        self.add_argument("root","str",None,"root path")
        self.add_argument("template","str",None,"template name")
        self.add_argument("size","int",10000,"verbose level")
        self.add_argument("source","str",None,"source path")
        self.add_argument("target","str",".","target path")
        self.add_argument("vertex_only",None,None,"generate vertex dataset only.")
        self.add_argument("verbose","int",0,"verbose level")
        pass

    def __run__(self,template,source,target,size,verbose,vertexOnly):
        ig_template.Data.Options.source = source
        ig_template.Data.Options.target = target
        ig_template.Data.Options.vertexOnly = vertexOnly
        parser = ig_template.Schema.Parser(template,None)
        parser.setVerboseLevel(verbose)
        parser.generateDataset(size)
        ig_template.Data.Options.CloseAllFiles()
        return True
        
    def operate(self):
        if operations.operation.operate(self):
            rootPath = self.getSingleOption("root")
            template = self.getSingleOption("template")
            size = self.getSingleOption("size")
            source = self.getSingleOption("source")
            target = self.getSingleOption("target")
                        
            if rootPath == None:
                self.error("Root path is not given.")
                return False
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
            return self.__run__(template,source,target,size,self.verbose,self.hasOption("vertex_only"))
        return True
