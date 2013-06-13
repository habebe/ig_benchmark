import os
import sys
import getopt
import base
import config
import util

class operation(base.operation):
    def __init__(self):
        base.operation.__init__(self)
        self.add_argument("detail","int",0,"detail level")
        pass

    def exists(self,path):
        if os.path.exists(config.ig.Config.BootFilePath):
            return self.output_string("[exists]",base.Colors.Green,True)
        return self.output_string("[does not exists]",base.Colors.Red,False)

    def operate(self):
        if base.operation.operate(self):
            self.detail = self.getSingleOption("detail")
            print self.output_string("Benchmark source configuration",base.Colors.Purple,True)
            print self.output_string("\tSourcePath=%s"%(config.ig.SourcePath),base.Colors.Blue,False)
            print self.output_string("\tBuildPath=%s"%(config.ig.BuildPath),base.Colors.Blue,False)
            print self.output_string("\tIG2_jar=%s"%(config.ig.IG2_jar),base.Colors.Blue,False),self.exists(config.ig.IG2_jar)
            print self.output_string("\tIG3_jar=%s"%(config.ig.IG3_jar),base.Colors.Blue,False),self.exists(config.ig.IG3_jar)
            print
            print self.output_string("IG configuration",base.Colors.Purple,True)
            print self.output_string("\tBootFilePath=%s"%(config.ig.Config.BootFilePath),base.Colors.Blue,False),self.exists(config.ig.Config.BootFilePath)
            print self.output_string("\tIG2_home=%s"%(config.ig.Config.IG2_Root),base.Colors.Blue,False),self.exists(config.ig.Config.IG2_Root)
            print self.output_string("\tIG3_home=%s"%(config.ig.Config.IG3_Root),base.Colors.Blue,False),self.exists(config.ig.Config.IG3_Root)
            print self.output_string("\tHost=%s"%(config.ig.Config.Host),base.Colors.Blue,False)
            (status,stdout,stderr) = util.check_lock_server.run(config.ig.Config.Host)
            print self.output_string("\tLock server status",base.Colors.Purple,True)
            for line in stdout:
                line = line.strip()
                if len(line):
                    print self.output_string("\t\t%s"%(line),base.Colors.Blue,False)
                    pass
                pass
            (status,stdout,stderr) = util.check_ams.run(config.ig.Config.Host)
            print self.output_string("\tAMS status",base.Colors.Purple,True)
            for line in stdout:
                line = line.strip()
                if len(line):
                    print self.output_string("\t\t%s"%(line),base.Colors.Blue,False)
                    pass
                pass
            print self.output_string("\tDisks",base.Colors.Purple,True)
            for i in config.ig.Config.Disks:
                print self.output_string("\t\t(name=%s,host=%s,device=%s)"%(i.name,i.host,i.device),base.Colors.Blue,False)
                pass
            return True
        return False
