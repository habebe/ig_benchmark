import xml.dom.minidom
import sys
import json
import os

class ConfigList:
    CONFIG = "Config"
    ENGINE = "Engine"
    ENGINELIST = "EngineList"
    
    def __init__(self,dom_object,name):
        self.dom_object = dom_object
        self.configs = []
        self.engines = []
        self.name = name
        pass

    def __repr__(self):
        buf = "ConfigList\n"
        for i in self.configs:
            buf += "\t{0}".format(str(i))
            pass
        for i in self.engines:
            buf += "\t{0}".format(str(i))
            pass
        return buf

    def build(self):
        elements = self.dom_object.getElementsByTagName(self.ENGINELIST)
        if len(elements):
            elements = elements[0]
            elements = self.dom_object.getElementsByTagName(self.ENGINE)
            for element in elements:
                engine = Engine(element)
                if not engine.build():
                    return False
                self.engines.append(engine)
                pass
            pass
        elements = self.dom_object.getElementsByTagName(self.CONFIG)
        for element in elements:
            config = Config(element,self)
            if not config.build():
                return False
            self.configs.append(config)
            pass
        return True
    pass

class Engine:
    VERSION = "version"
    HOME    = "home"
    
    def __init__(self,dom_object):
        self.dom_object = dom_object
        self.version = None
        self.home = None
        pass

    def __repr__(self):
        buf = "Engine {0}: root:{1}\n".format(self.version,self.home)
        return buf

    def build(self):
        self.version = self.dom_object.getAttribute(self.VERSION)
        self.home    = self.dom_object.getAttribute(self.HOME)
        return True
    pass

class Config:
    LOCKSERVER = "Lockserver"
    ADDRESS = "address"
    HOST = "Host"
    NAME = "name"
    
    def __init__(self,dom_object,parent):
        self.dom_object = dom_object
        self.lockserver = None
        self.hosts = []
        self.name = None
        self.parent = parent
        pass

    def getFullName(self):
        return "{0}:{1}".format(self.parent.name,self.name)

    def __repr__(self):
        buf = "Config {0}: Lock-server {1}\n".format(self.name,self.lockserver)
        for i in self.hosts:
            buf += "\t{0}".format(i)
        return buf

    def build(self):
        self.name = self.dom_object.getAttribute(self.NAME)
        elements = self.dom_object.getElementsByTagName(self.LOCKSERVER)
        if len(elements):
            element = elements[0]
            self.lockserver = element.getAttribute(self.ADDRESS)
            pass
        elements = self.dom_object.getElementsByTagName(self.HOST)
        for i in elements:
            if i.parentNode == self.dom_object:
                host = Host(i)
                if not host.build():
                    return False
                self.hosts.append(host)
                pass
        return True
    pass

class Host:
    ADDRESS = "address"
    DISK = "Disk"
    
    def __init__(self,dom_object):
        self.dom_object = dom_object
        self.address = None
        self.disks = []
        pass


    def __repr__(self):
        buf = "Host {0}\n".format(self.address)
        for i in self.disks:
            buf += "\t{0}\n".format(str(i))
            pass
        return buf

    def build(self):
        self.address = self.dom_object.getAttribute(self.ADDRESS)
        elements = self.dom_object.getElementsByTagName(self.DISK)
        for i in elements:
            if i.parentNode == self.dom_object:
                disk = Disk(i)
                if not disk.build():
                    return False
                self.disks.append(disk)
                pass
        return True
    pass


class Disk:
    LOCATION = "location"
    def __init__(self,dom_object):
        self.dom_object = dom_object
        self.location = None
        pass

    def __repr__(self):
        buf = "\tDisk:{0}".format(self.location)
        return buf

    def build(self):
        self.location = self.dom_object.getAttribute(self.LOCATION)
        return True
    
    pass


def parse(fileName,handler):
    document = None
    try:
        document = xml.dom.minidom.parse(fileName)
    except xml.parsers.expat.ExpatError as e:
        message = "while parsing template file '{0}'\n\t{1}".format(fileName,str(e))
        if handler:
            handler.error(message)
        else:
            print message
        return None
    dom_object = document.getElementsByTagName("ConfigList")
    if len(dom_object):
        name = os.path.basename(fileName)
        if name.endswith(".xml"):
            name = name.replace(".xml","")
            pass
        configList = ConfigList(dom_object[0],name)
        configList.build()
        return configList
    return None


if __name__ == "__main__":
    c = parse(sys.argv[1],None)
    print str(c)
    pass

