import os
import sys
import base
import platform
import shutil
import subprocess
import make_graph500
import string

class operation(base.operation):
    def __init__(self):
        base.operation.__init__(self)
        self.add_argument("scale","int",8,"scale")
        self.add_argument("factor","int",8,"edge factor")
        self.add_argument("a","float",0.25,"a")
        self.add_argument("b","float",0.25,"b")
        self.add_argument("c","float",0.25,"c")
        self.add_argument("d","float",0.25,"d")
        self.add_argument("output","str","edgelist.txt","edgelist file name")
        pass

    def run(self,scale,factor,a,b,c,d,output):
        make = make_graph500.operation()
        make.run()
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        graph_dir = os.path.join(base_dir,"graph500-2.1.4")
        exe = os.path.join(graph_dir,"make-edgelist")
        arguments = [exe,"-s",str(scale),"-e",str(factor),"-a",str(a),"-b",str(b),"-c",str(c),"-d",str(d),"-o",output]
        print "Generate elist ",string.join(arguments)
        p = subprocess.Popen(arguments,stdout=sys.stdout,stderr=sys.stderr)
        p.wait()
        return True

    def operate(self):
        if base.operation.operate(self):
            self.scale = self.getSingleOption("scale")
            self.factor = self.getSingleOption("factor")
            self.a = self.getSingleOption("a")
            self.b = self.getSingleOption("b")
            self.c = self.getSingleOption("c")
            self.d = self.getSingleOption("d")
            self.output = self.getSingleOption("output")
            return self.run(self.scale,self.factor,self.a,self.b,self.c,self.d,self.output)
        return False
