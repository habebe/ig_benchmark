import os
import sys
import getopt
import operations
import shutil
import subprocess
import string
import config
import Service

class operation(operations.operation):
    "Start and stop benchmark service."
    def __init__(self):
        operations.operation.__init__(self,"service",False)
        self.add_argument("help",None,None,"show help message")
        self.add_argument("root","str",".","Root path.")
        self.add_argument("start",None,None,"Start service")
        self.add_argument("stop",None,None,"Stop service on given host")
        self.add_argument("host","str",None,"Host name")
        self.add_argument("request","str",None,"Service request")
        self.add_argument("arg","str",None,"Argument name")
        self.add_argument("verbose","int","0","Verbose level.")
        pass
            
    def operate(self):
        if operations.operation.operate(self):
            rootPath = self.getSingleOption("root")
            if self.hasOption("start"):
                server = Service.Server()
                if server.init():
                    server.run()
                    return True
                    pass
                pass
            elif self.hasOption("stop"):
                host = self.getSingleOption("host")
                if host == None:
                    print "Host name is not given"
                    return False
                request = Service.Request(host)
                if request.init():
                    data = request.stop_service()
                    print "Response:",data
                    request.close()
                    return True
                return False
            elif self.hasOption("request"):
                host = self.getSingleOption("host")
                R = self.getSingleOption("request")
                args = self.getOption("arg")
                if host == None:
                    print "Host name is not given"
                    return False
                request = Service.Request(host)
                if request.init():
                    request.request(R,args)
                    request.start()
                    request.join()
                    print "Threaded Response:",request.response
                    request.close()
                    return True
                return False
                pass
            pass
        self.error("Unknown error: service.")
        return False
    pass
