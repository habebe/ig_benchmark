from db_types import *
import types
import json

class db_object:
    @classmethod
    def create_table(self,database,cursor):
        database.create_table(self,cursor)
        return

    @classmethod
    def get_name(self):
        return "_%s"%(self.__name__)

    @classmethod
    def create_object(self,data):
        return self().set_data(data)

    _property_ = [
        ("id",db_types.PRIMARY_KEY)
        ]

    def __init__(self):
        self.id = None
        pass

    def set_data(self,data,removeUnicode = False):
        if removeUnicode:
            data = self.remove_unicode(data)
            pass
        (self.id,) = data
        return self

    def get_data(self,includeId):
        if includeId:
            return (self.id,)
        return ()
    
    def remove_unicode(self,_data):
        data = ()
        for i in _data:
            if type(i) == types.UnicodeType:
                data += (str(i),)
            else:
                data += (i,)
                pass
            pass
        return data

    @classmethod
    def fetch_using(self,property,value):
        statement = "SELECT * from %s where %s = %s"%(self.get_name(),property,str(value)) 
        print statement
        
    pass


class _name_(db_object):
    _property_ = db_object._property_ + [
        ("name",db_types.TEXT)
        ]
    
    def __init__(self):
        db_object.__init__(self)
        self.name = None
        pass
    
    def set_data(self,data,removeUnicode = True):
        if removeUnicode:
            data = self.remove_unicode(data)
            pass
        parent_data = data[:(len(db_object._property_))]
        self_data = data[(len(db_object._property_)):]
        o = db_object.set_data(self,parent_data,False)
        (self.name,) = self_data
        return self
    
    def get_data(self,includeId):
        data = db_object.get_data(self,includeId)
        data += (self.name,)
        return data    
    pass


class _name_description_(_name_):
    _property_ = _name_._property_ + [
        ("description",db_types.TEXT)
        ]

    def __init__(self):
        _name_.__init__(self)
        self.description = None
        pass

    def set_data(self,data,removeUnicode = True):
        if removeUnicode:
            data = self.remove_unicode(data)
        parent_data = data[:(len(_name_._property_))]
        self_data = data[(len(_name_._property_)):]
        o = _name_.set_data(self,parent_data,False)
        (self.description,) = self_data
        return self

    def get_data(self,includeId):
        data = _name_.get_data(self,includeId)
        data += (self.description,)
        return data
    pass

class engine(_name_description_):
    def __init__(self):
        _name_description_.__init__(self)
        pass

class simtype(_name_description_):
    def __init__(self):
        _name_description_.__init__(self)
        pass
    pass

class config(_name_description_):
    def __init__(self):
        _name_description_.__init__(self)
        pass
    pass

class tag(_name_):
    _property_ = _name_._property_ + [
        ("timestamp",db_types.DATETIME),
        ]
    
    def __init__(self):
        _name_.__init__(self)
        self.timestamp = None
        pass

    def set_data(self,data,removeUnicode = True):
        if removeUnicode:
            data = self.remove_unicode(data)
        parent_data = data[:(len(_name_._property_))]
        self_data = data[(len(_name_._property_)):]
        o = _name_.set_data(self,parent_data,False)
        (self.timestamp,) = self_data
        return self

    def get_data(self,includeId):
        data = _name_.get_data(self,includeId)
        data += (self.timestamp,)
        return data
    pass

class case_type(_name_description_):
    def __init__(self):
        _name_description_.__init__(self)
        pass
    pass

class platform(_name_):
    _property_ = _name_._property_ + [ ("type",db_types.TEXT) ]
    
    def __init__(self):
        _name_.__init__(self)
        self.type = None
        pass
    
    def set_data(self,data,removeUnicode = True):
        if removeUnicode:
            data = self.remove_unicode(data)
            pass
        parent_data = data[:(len(_name_._property_))]
        self_data = data[(len(_name_._property_)):]
        o = _name_.set_data(self,parent_data)
        (self.type,) = self_data
        return self

    def get_data(self,includeId):
        data = _name_.get_data(self,includeId)
        data += (self.type,)
        return data
    pass


class index_type(_name_):
    def __init__(self):
        _name_.__init__(self)
        pass
    pass
            
    
class suite(_name_description_):
    _property_ = _name_description_._property_ + [
        ("path",db_types.TEXT),
        ("parent",db_types.INTEGER)
        ]

    RootSuite = None
    
    def __init__(self):
        _name_description_.__init__(self)
        self.path = None
        self.parent = -1
        self.t_cases = []
        self.t_sub_suites = []
        pass

    def object_type(self):
        return "suite"

    def run(self,db,**kwargs):
        print "suite: ",self.name
        for i in self.t_cases:
            i.run(db,self,**kwargs)
        for i in self.t_sub_suites:
            i.run(db,**kwargs)

    def setParent(self,suiteObject):
        if suiteObject:
            self.parent = suiteObject.id
        else:
            self.parent = None
            pass
        return

       
    def set_data(self,data,removeUnicode = True):
        if removeUnicode:
            data = self.remove_unicode(data)
            pass
        parent_data = data[:(len(_name_description_._property_))]
        self_data = data[(len(_name_description_._property_)):]
        o = _name_description_.set_data(self,parent_data)
        (self.path,self.parent) = self_data
        return self

    def get_data(self,includeId):
        data = _name_description_.get_data(self,includeId)
        data += (self.path,self.parent)
        return data
    pass


class case(suite):
    _property_ = suite._property_ + [
        ("case_type_id",db_types.INTEGER),
        ("table_view",db_types.TEXT),
        ("plot_view",db_types.TEXT),
        ("data",db_types.TEXT)
        ]
    
    def __init__(self):
        suite.__init__(self)
        self.case_type_id = None
        self.table_view = None
        self.plot_view = None
        self.data = None
        self.t_operation = None
        pass

    def object_type(self):
        return "case"
    
    def run(self,db,parent,**kwargs):
        print "\tcase: ",self.name," operation:",self.t_operation.name
        self.t_operation.run(db,parent,self,eval(self.data),**kwargs)
        pass            

    def setCaseType(self,case_type_object):
        if case_type_object:
            self.case_type_id = case_type_object.id
        else:
            self.case_type_id = None
            pass
        return

    def set_data(self,data,removeUnicode = True):
        if removeUnicode:
            data = self.remove_unicode(data)
            pass
        parent_data = data[:(len(suite._property_))]
        self_data = data[(len(suite._property_)):]
        o = suite.set_data(self,parent_data)
        (o.case_type_id,o.table_view,o.plot_view,o.data) = self_data
        return self

    def get_data(self,includeId):
        data = suite.get_data(self,includeId)
        data += (self.case_type_id,self.table_view,self.plot_view,self.data)
        return data
    pass


class case_data_stat(db_object):
    _property_ = db_object._property_ + [
        ("case_id",db_types.INTEGER),
        ("key",db_types.TEXT),
        ("counter",db_types.INTEGER),
        
        ("time_min",db_types.REAL),
        ("time_max",db_types.REAL),
        ("time_sum",db_types.REAL),

        ("rate_min",db_types.REAL),
        ("rate_max",db_types.REAL),
        ("rate_sum",db_types.REAL),
        
        ("memory_init_min",db_types.REAL),
        ("memory_init_max",db_types.REAL),
        ("memory_init_sum",db_types.REAL),
        
        ("memory_used_min",db_types.REAL),
        ("memory_used_max",db_types.REAL),
        ("memory_used_sum",db_types.REAL),

        ("memory_committed_min",db_types.REAL),
        ("memory_committed_max",db_types.REAL),
        ("memory_committed_sum",db_types.REAL),
    
        ("memory_max_min",db_types.REAL),
        ("memory_max_max",db_types.REAL),
        ("memory_max_sum",db_types.REAL),
        ]

    def __init__(self):
        db_object.__init__(self)
        self.case_id = None
        self.key = None
        self.key_data = None
        self.counter = 0
        
        self.time_min = None
        self.time_max = None
        self.time_sum = None

        self.rate_min = None
        self.rate_max = None
        self.rate_sum = None
        
        self.memory_init_min = None
        self.memory_init_max = None
        self.memory_init_sum = None
        
        self.memory_used_min = None
        self.memory_used_max = None
        self.memory_used_sum = None

        self.memory_committed_min = None
        self.memory_committed_max = None
        self.memory_committed_sum = None
     
        self.memory_max_min = None
        self.memory_max_max = None
        self.memory_max_sum = None

        self.mapper = None
        pass

    def generate_key_data(self):
        if self.key_data == None:
            self.key_data = json.loads(self.key)
            pass
        pass

    def op_size(self):
        self.generate_key_data()
        value = self.key_data[3]
        return value

    def graph_size(self):
        self.generate_key_data()
        value = self.key_data[2]
        return value

        
    def platform_id(self):
        self.generate_key_data()
        value = self.key_data[8]
        return value
    
    def platform(self):
        value = self.platform_id()
        if self.mapper:
            return self.mapper.PLATFORM_MAP[value]
        return str(value)

    def engine_id(self):
        self.generate_key_data()
        value = self.key_data[1]
        return value

    def engine(self):
        value = self.engine_id()
        if self.mapper and self.mapper.ENGINE_MAP.has_key(value):
            return self.mapper.ENGINE_MAP[value]
        return str(value)

    def threads(self):
        self.generate_key_data()
        value = self.key_data[9]
        return value

    def index_type_id(self):
        self.generate_key_data()
        value = self.key_data[10]
        return value

   
    def object_data(self,name,default_value):
        self.generate_key_data()
        if len(self.key_data) >= 13:
            data = self.key_data[12]
            if data.has_key(name):
                return data[name]
            pass
        return default_value

    def index_type(self):
        value = self.index_type_id()
        if self.mapper:
            return self.mapper.INDEX_MAP[value]
        return str(value)

    def config_id(self):
        self.generate_key_data()
        value = self.key_data[11]
        return value

    def config(self):
        value = self.config_id()
        if self.mapper and self.mapper.CONFIG_MAP.has_key(value):
            return self.mapper.CONFIG_MAP[value]
        return str(value)

    def cache_init(self):
        self.generate_key_data()
        return self.key_data[6]

    def cache_max(self):
        self.generate_key_data()
        return self.key_data[7]
    
    def page_size(self):
        self.generate_key_data()
        return self.key_data[5]

    def tx_size(self):
        self.generate_key_data()
        return self.key_data[4]
    
        
    def rate_avg(self):
        return self.rate_sum/self.counter

    def time_avg(self):
        return self.time_sum/self.counter

    def memory_max_avg(self):
        return self.memory_max_sum/self.counter

    def memory_used_avg(self):
        return self.memory_used_sum/self.counter


    def addCounter(self):
        if self.counter == None:
            self.counter = 0
            pass
        self.counter += 1

    def getStat(self,value,minValue,maxValue,sumValue):
        if minValue == None:
            minValue = value
        else:
            minValue = min(value,minValue)
            pass
        if maxValue == None:
            maxValue = value
        else:
            maxValue = max(value,maxValue)
            pass
        if sumValue == None:
            sumValue = 0
            pass
        sumValue = sumValue + value
        return (minValue,maxValue,sumValue)


    def setTimeStat(self,value):
        (self.time_min,self.time_max,self.time_sum) = self.getStat(value,self.time_min,self.time_max,self.time_sum)
        pass

    def setRateStat(self,value):
        (self.rate_min,self.rate_max,self.rate_sum) = self.getStat(value,self.rate_min,self.rate_max,self.rate_sum)
        pass
    
    def setMemInitStat(self,value):
        (self.memory_init_min,self.memory_init_max,self.memory_init_sum) = self.getStat(value,self.memory_init_min,self.memory_init_max,self.memory_init_sum)
        pass

    def setMemUsedStat(self,value):
        (self.memory_used_min,self.memory_used_max,self.memory_used_sum) = self.getStat(value,self.memory_used_min,self.memory_used_max,self.memory_used_sum)
        pass

    def setMemCommittedStat(self,value):
        (self.memory_committed_min,self.memory_committed_max,self.memory_committed_sum) = self.getStat(value,self.memory_committed_min,self.memory_committed_max,self.memory_committed_sum)
        pass

    def setMemMaxStat(self,value):
        (self.memory_max_min,self.memory_max_max,self.memory_max_sum) = self.getStat(value,self.memory_max_min,self.memory_max_max,self.memory_max_sum)
        pass
    
    def set_data(self,data,removeUnicode = True):
        if removeUnicode:
            data = self.remove_unicode(data)
            pass
        parent_data = data[:(len(db_object._property_))]
        self_data = data[(len(db_object._property_)):]
        o = db_object.set_data(self,parent_data,False)
        (
            self.case_id,
            self.key,
            self.counter,
            self.time_min,
            self.time_max,
            self.time_sum,
            self.rate_min,
            self.rate_max,
            self.rate_sum,
            self.memory_init_min,
            self.memory_init_max,
            self.memory_init_sum,
            self.memory_used_min,
            self.memory_used_max,
            self.memory_used_sum,
            self.memory_committed_min,
            self.memory_committed_max,
            self.memory_committed_sum,
            self.memory_max_min,
            self.memory_max_max,
            self.memory_max_sum
            ) = self_data
        
        return self
    
    def get_data(self,includeId):
        data = db_object.get_data(self,includeId)
        data += (
            self.case_id,
            self.key,
            self.counter,
            self.time_min,
            self.time_max,
            self.time_sum,
            self.rate_min,
            self.rate_max,
            self.rate_sum,
            self.memory_init_min,
            self.memory_init_max,
            self.memory_init_sum,
            self.memory_used_min,
            self.memory_used_max,
            self.memory_used_sum,
            self.memory_committed_min,
            self.memory_committed_max,
            self.memory_committed_sum,
            self.memory_max_min,
            self.memory_max_max,
            self.memory_max_sum
            )
        return data
        

class case_data(db_object):
    _property_ = db_object._property_ + [
        ("timestamp",db_types.DATETIME),
        ("tag_id",db_types.INTEGER),
        ("status",db_types.INTEGER),
        
        ("case_id",db_types.INTEGER),
        ("engine_id",db_types.INTEGER),
        ("size",db_types.INTEGER),
        ("op_size",db_types.INTEGER),
        ("tx_size",db_types.INTEGER),
        
        ("page_size",db_types.INTEGER),
        ("cache_init",db_types.INTEGER),
        ("cache_max",db_types.INTEGER),
        
        ("platform_id",db_types.INTEGER),
        ("threads",db_types.INTEGER),
        ("index_id",db_types.INTEGER),
        ("config_id",db_types.INTEGER),
        
        ("time",db_types.REAL),
        ("rate",db_types.REAL),
        ("memory_init",db_types.REAL),
        ("memory_used",db_types.REAL),
        ("memory_committed",db_types.REAL),
        ("memory_max",db_types.REAL),
        ("data",db_types.TEXT),
        ]

    def __init__(self):
        db_object.__init__(self)
        self.timestamp = None
        self.tag_id = None
        self.status = None
        
        self.case_id = None
        self.engine_id = None
        self.size = None
        self.op_size = None
        self.tx_size = None
        
        self.page_size = None
        
        
        self.cache_init = None
        self.cache_max = None
        
        self.platform_id = None
        self.threads = None
        self.index_id = None
        self.config_id = None
        
        self.time = None
        self.rate = None
        self.memory_init = None
        self.memory_used = None
        self.memory_committed = None
        self.memory_max = None
                
        self.data = None
        pass

    def generateKey(self):
        return json.dumps(
            [self.case_id,self.engine_id,
             self.size,self.op_size,self.tx_size,
             self.page_size,self.cache_init,self.cache_max,
             self.platform_id,self.threads,self.index_id,self.config_id,
             self.data
             ]
            )
    
    def setDataValue(self,name,value):
        if self.data == None:
            self.data = {}
            pass
        self.data[name] = value
        pass

    def getDataValue(self,name):
        value = None
        if self.data:
            if self.data.has_key(name):
                value = data[name]
                pass
            pass
        return value
    
    def setCase(self,_object_):
        self.case_id = _object_.id
        return self

    def setTag(self,_object_):
        self.tag_id = _object_.id
        return self

    def setEngine(self,_object_):
        self.engine_id = _object_.id
        return self
    
    def set_data(self,data,removeUnicode = True):
        if removeUnicode:
            data = self.remove_unicode(data)
            pass
        parent_data = data[:(len(db_object._property_))]
        self_data = data[(len(db_object._property_)):]
        o = db_object.set_data(self,parent_data,False)
        (
            self.timestamp,self.tag_id,self.status,
            self.case_id,self.engine_id,
            self.size,self.op_size,self.tx_size,
            self.page_size,self.cache_init,self.cache_max,
            self.platform_id,self.threads,self.index_id,self.config_id,
            self.time,self.rate,
            self.memory_init,self.memory_used,self.memory_committed,self.memory_max,
            self.data ) = self_data
        if self.data == None:
            self.data = {}
        else:
            self.data = json.loads(self.data)
            pass
        return self
    
    def get_data(self,includeId):
        data = db_object.get_data(self,includeId)
        if self.data == None:
            self.data = {}
            pass
        data += (
            self.timestamp,self.tag_id,self.status,
            self.case_id,self.engine_id,
            self.size,self.op_size,self.tx_size,
            self.page_size,self.cache_init,self.cache_max,
            self.platform_id,self.threads,self.index_id,self.config_id,
            self.time,self.rate,
            self.memory_init,self.memory_used,self.memory_committed,self.memory_max,
            json.dumps(self.data))
        return data
    
    pass


