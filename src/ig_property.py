import random

class __Defaults__:
    Defaults = {
        "IG.BootFilePath":".",
        "IG.LockServerHost":"127.0.0.1",
        "IG.UseMROWTransactions":"false",
        "IG.Indexing.GraphIndexSensitive":"true",
        "IG.SessionPool.ThreadBasedSessionPool.InitialCacheSizeKb":1000,
        "IG.SessionPool.ThreadBasedSessionPool.MaximumCacheSizeKb":5000000,
        "IG.SessionPool.ThreadBasedSessionPool.LockWaitTime":10,
        "IG.SessionPool.ThreadBasedSessionPool.SizeHardLimit":256,
        "IG.PageSize":16384
        }
    
class PropertyFile:
    def __init__(self,fileName="IG.properties",defaults=__Defaults__.Defaults):
        self.properties = defaults.copy()
        self.defaults = defaults.copy()
        self.fileName = fileName
        pass

    def setBootPath(self,value):
        self.properties["IG.BootFilePath"] = value
        pass

    def setLockServer(self,value):
        self.properties["IG.LockServerHost"] = value
        pass

    def setInitCache(self,value):
        self.properties["IG.SessionPool.ThreadBasedSessionPool.InitialCacheSizeKb"] = value
        pass

    def setMaxCache(self,value):
        self.properties["IG.SessionPool.ThreadBasedSessionPool.MaximumCacheSizeKb"] = value
        pass

    def setPageSize(self,value):
        self.properties["IG.PageSize"] = value
        pass
    
    def getInitCache(self):
        return self.properties["IG.SessionPool.ThreadBasedSessionPool.InitialCacheSizeKb"]
        
    def getMaxCache(self):
        return self.properties["IG.SessionPool.ThreadBasedSessionPool.MaximumCacheSizeKb"]

    
    def _initialize_(self):
        self.properties = self.defaults.copy()
        self.properties["IG.InstanceId"] = random.randint(1,65535-1)
        pass

    def generate(self):
        f = file(self.fileName,"w")
        for key in self.properties.keys():
            value = self.properties[key]
            print >> f,str(key)+"="+str(value)
        f.flush()
        f.close()


class LocationConfigFile:
    def __init__(self,fileName):
        self.fileName = fileName
        pass

    def generate(self,disks):
        f = file(self.fileName,"w")
        print >> f,'<?xml version="1.0" encoding="UTF-8"?>'
        print >> f,'<InfiniteGraph>'
        print >> f,'<LocationPreferences allowNonPreferredLocations="true">'
        print >> f,'<LocationPreferenceRank>'
        for disk in disks:
            print >> f,'<StorageLocation value="%s"/>'%(disk[0])
            pass
        print >> f,'</LocationPreferenceRank>'
        print >> f,'</LocationPreferences>'
        print >> f,'</InfiniteGraph>'

        f.flush()
        f.close()
        pass
    pass

#p = IG_PropertyFile()
#p.generate()


        
