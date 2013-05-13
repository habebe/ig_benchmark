import os

class Disk:
    def __init__(self,name,host,device):
        self.name = name
        self.host = host
        self.device = device
        pass
    pass

class LocalConfig:
    BootFilePath = {
        "ig2":os.path.expanduser("/disk1/IG2_data/data/"),
        "ig3":os.path.expanduser("/disk1/IG3_data/data/"),
        }
    
    Disks = {
        "ig2":[Disk("disk1","127.0.0.1",os.path.expanduser("/disk1/IG2_data/data/"))],
        "ig3":[Disk("disk1","127.0.0.1",os.path.expanduser("/disk1/IG3_data/data/"))],
        }
    Root = {
        "ig2":"/opt/local/InfiniteGraph-2.1.0_rc2/mac86_64/",
        "ig3":"/Applications/InfiniteGraph/3.0"
        }
    Host = {
        "ig2":"127.0.0.1",
        "ig3":"127.0.0.1"
        }
    SourcePath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    BuildPath = os.path.join(SourcePath,"build")
    BenchmarkJar = {
        "ig2":os.path.join(BuildPath,"benchmark.2.jar"),
        "ig3":os.path.join(BuildPath,"benchmark.3.jar"),
        }
    DiskMap = {}
    @classmethod 
    def GetDiskMap(self):
        if len(LocalConfig.DiskMap) == 0:
            LocalConfig.DiskMap[1] = LocalConfig.Disks
            pass
        return LocalConfig.DiskMap
    RemoteHost = {
        "ig2":"frak08-b11.objy.com",
        "ig3":"frak08-b11.objy.com"
        }
    HostMap = {}
    @classmethod
    def GetHostMap(self):
        if len(FrakConfig.HostMap) == 0:
            FrakConfig.HostMap["local"]  = FrakConfig.Host
            FrakConfig.HostMap["remote"] = FrakConfig.RemoteHost
            pass
        return FrakConfig.HostMap
    pass

class FrakConfig:
    BootFilePath = {
        "ig2":os.path.expanduser("/disk1/IG2_data/data/"),
        "ig3":os.path.expanduser("/disk1/IG3_data/data/"),
        }
    
    Disks = {
        "ig2":[Disk("disk1","frak08-b11.objy.com","/disk1/IG2_data/data/")],
        "ig3":[Disk("disk1","frak08-b11.objy.com","/disk2/IG3_data/data/")],
        }
    Disks_2 = {
        "ig2":[
            Disk("disk1","frak08-b11.objy.com","/disk1/IG2_data/data/"),
            Disk("disk2","frak08-b11.objy.com","/disk2/IG2_data/data/"),
        ],
        "ig3":[
            Disk("disk1","frak08-b11.objy.com","/disk2/IG3_data/data/"),
            Disk("disk2","frak08-b11.objy.com","/disk2/IG3_data/data/"),
            ]
        }
    Disks_3 = {
        "ig2":[
            Disk("disk1","frak08-b11.objy.com","/disk1/IG2_data/data/"),
            Disk("disk2","frak08-b11.objy.com","/disk2/IG2_data/data/"),
            Disk("disk3","frak08-b11.objy.com","/disk3/IG2_data/data/"),
            ],
        "ig3":[
            Disk("disk1","frak08-b11.objy.com","/disk1/IG3_data/data/"),
            Disk("disk2","frak08-b11.objy.com","/disk2/IG3_data/data/"),
            Disk("disk3","frak08-b11.objy.com","/disk3/IG3_data/data/"),
            ]
        }
    Disks_4 = {
        "ig2":[
            Disk("disk1","frak08-b11.objy.com","/disk1/IG2_data/data/"),
            Disk("disk2","frak08-b11.objy.com","/disk2/IG2_data/data/"),
            Disk("disk3","frak08-b11.objy.com","/disk3/IG2_data/data/"),
            Disk("disk4","frak08-b11.objy.com","/disk4/IG2_data/data/"),
            ],
        "ig3":[
            Disk("disk1","frak08-b11.objy.com","/disk1/IG3_data/data/"),
            Disk("disk2","frak08-b11.objy.com","/disk2/IG3_data/data/"),
            Disk("disk3","frak08-b11.objy.com","/disk3/IG3_data/data/"),
            Disk("disk4","frak08-b11.objy.com","/disk4/IG3_data/data/"),
            ]
        }

    DiskMap = {}

    @classmethod 
    def GetDiskMap(self):
        if len(FrakConfig.DiskMap) == 0:
            FrakConfig.DiskMap[1] = FrakConfig.Disks
            FrakConfig.DiskMap[2] = FrakConfig.Disks_2
            FrakConfig.DiskMap[3] = FrakConfig.Disks_3
            FrakConfig.DiskMap[4] = FrakConfig.Disks_4
        return FrakConfig.DiskMap
        
    Root = {
        "ig2":"/disk1/InfiniteGraph-2.1.0/linux86_64/",
        "ig3":"/disk1/InfiniteGraph/3.0/"
        }
    Host = {
        "ig2":"127.0.0.1",
        "ig3":"127.0.0.1"
        }
    RemoteHost = {
        "ig2":"frak08-b11.objy.com",
        "ig3":"frak08-b11.objy.com"
        }
    HostMap = {}
    @classmethod
    def GetHostMap(self):
        if len(FrakConfig.HostMap) == 0:
            FrakConfig.HostMap["local"]  = FrakConfig.Host
            FrakConfig.HostMap["remote"] = FrakConfig.RemoteHost
            pass
        return FrakConfig.HostMap
    
    SourcePath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    BuildPath = os.path.join(SourcePath,"build")
    BenchmarkJar = {
        "ig2":os.path.join(BuildPath,"benchmark.2.jar"),
        "ig3":os.path.join(BuildPath,"benchmark.3.jar"),
        }
    pass


class FrakConfig__:
    BootFilePath = "/disk1/IG_data/"
    Disks = [
        Disk("disk1","frak08-b11.objy.com","/disk1/IG_data/")
        ]
    IG2_Root = "/disk1/InfiniteGraph-2.1.0/linux86_64/"
    IG3_Root = "/disk1/InfiniteGraph/3.0/"
    Host = "127.0.0.1"
    pass






Config = LocalConfig
#Config = FrakConfig


def Setup():
    os.system("mkdir -p %s"%(Config.BootFilePath))
    for i in Config.Disks:
        os.system("mkdir -p %s"%(i.device))
        pass
    pass

