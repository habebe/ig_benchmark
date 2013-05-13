import imp
import os
import types
import sys

class suite_info:
    BASE_FILE_NAME = "__suite__.py"

    def __init__(self):
        self.parent = None
        self.file_name = None
        self.is_root = None
        self.suites = []
        self.suite_map = {}
        self.cases = []
        self.name = None
        self.description = None
        self.id = -1
        self.problem_size = {}
        self.default_problem_size = None
        pass

    def get_path(self):
        return os.path.dirname(self.file_name)
    
    def get_name(self):
        if self.name:
            return self.name
        return os.path.basename(self.get_path())

    def __read_default_problem_size__(self,module,handler):
        try:
            self.default_problem_size = module.default_problem_size
            self.default_problem_size = str(self.default_problem_size)
            pass
        except Exception, e:
            self.default_problem_size = None
            pass
        return True

    def __read_name__(self,module,handler):
        try:
            self.name = module.name
            self.name = str(self.name)
            pass
        except Exception, e:
            print e
            self.name = None
            pass
        return True

    def __read_description__(self,module,handler):
        try:
            self.description = module.description
            self.description = str(self.description)
            pass
        except Exception, e:
            self.description = None
            pass
        return True
    
    def __read_is_root__(self,module,handler):
        _type_ = None
        try:
            self.is_root = module.is_root
            _type_ = type(self.is_root)
            #print _type_,self.is_root,((_type_ == types.BooleanType) or  (_type_ == types.IntType))
            if not ((_type_ == types.BooleanType) or (_type_ == types.IntType)):
                message  = "\n\tFile:%s\n"%(self.file_name)
                message += "\tExpected a boolean type for member 'is_root' but received type '%s'.\n"%(str(_type_))
                message += "\tSetting is_root to False."
                handler.warn(message)
                self.is_root = False
                pass
            pass
        except Exception, e:
            if 0:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message  = "\n\tFile:%s\n"%(self.file_name)
                message += "\tLine number:%d\n"%(exc_tb.tb_lineno)
                message += "\t%s\n"%(str(exc_obj))
                handler.error(message)
                pass
            self.is_root = False
            pass
        return True


    def __read_problem_size__(self,module,handler):
        _type_ = None
        try:
            problem_size = module.problem_size
            _type_ = type(problem_size)
            if not (_type_ == types.DictType):
                message  = "\n\tFile:%s\n"%(self.file_name)
                message += "\tExpected a dictionary type for member 'problem_size' but received type '%s'.\n"%(str(_type_))
                message += "\tIgnoring problem_size value."
                handler.warn(message)
            else:
                for item in problem_size:
                    self.problem_size[item] = problem_size[item]
                    pass
                pass
            pass
        except Exception, e:
            if 0:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message  = "\n\tFile:%s\n"%(self.file_name)
                message += "\tLine number:%d\n"%(exc_tb.tb_lineno)
                message += "\t%s\n"%(str(exc_obj))
                handler.error(message)
                pass
            self.suites = []
            pass
        return True

    def __read_suites__(self,module,handler):
        _type_ = None
        try:
            self.suites = module.suites
            _type_ = type(self.suites)
            #print _type_,self.is_root,(_type_ == types.ListType) 
            if not (_type_ == types.ListType):
                message  = "\n\tFile:%s\n"%(self.file_name)
                message += "\tExpected a list type for member 'suites' but received type '%s'.\n"%(str(_type_))
                message += "\tSetting suites to empty."
                handler.warn(message)
                self.suites = []
            else:
                counter = 0
                items = []
                for item in self.suites:
                    _type_ = type(item)        
                    if not (_type_ == types.StringType):
                        message  = "\n\tFile:%s\n"%(self.file_name)
                        message += "\tExpected a string type for list member of 'suites' at index %d but received type '%s'.\n"%(counter,str(_type_))
                        message += "\tIgnoring suite item (%s)."%(str(item))
                        handler.warn(message)
                    else:
                        items.append(item)
                        pass
                    counter += 1
                    pass
                self.suites = items
                pass
            pass
        except Exception, e:
            if 0:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message  = "\n\tFile:%s\n"%(self.file_name)
                message += "\tLine number:%d\n"%(exc_tb.tb_lineno)
                message += "\t%s\n"%(str(exc_obj))
                handler.error(message)
                pass
            self.suites = []
            pass
        return True


    def __read_cases__(self,module,handler):
        _type_ = None
        try:
            self.cases = module.cases
            _type_ = type(self.cases)
            #print _type_,self.is_root,(_type_ == types.ListType) 
            if not (_type_ == types.ListType):
                message  = "\n\tFile:%s\n"%(self.file_name)
                message += "\tExpected a list type for member 'cases' but received type '%s'.\n"%(str(_type_))
                message += "\tSetting cases to empty."
                handler.warn(message)
                self.cases = []
            else:
                counter = 0
                items = []
                for item in self.cases:
                    _type_ = type(item)        
                    if not (_type_ == types.DictType):
                        message  = "\n\tFile:%s\n"%(self.file_name)
                        message += "\tExpected a dictionary type for list member of 'cases' at index %d but received type '%s'.\n"%(counter,str(_type_))
                        message += "\tIgnoring case item (%s)."%(str(item))
                        handler.warn(message)
                    else:
                        items.append(item)
                        pass
                    counter += 1
                    pass
                self.cases = items
                pass
            pass
        except Exception, e:
            if 0:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                message  = "\n\tFile:%s\n"%(self.file_name)
                message += "\tLine number:%d\n"%(exc_tb.tb_lineno)
                message += "\t%s\n"%(str(exc_obj))
                handler.error(message)
                pass
            self.cases = []
            pass
        return True

    def __repr__(self):
        _buffer_ = '{"file_name":"%s","root":%d,"cases":%d,"suites":%d,"problem_size":%s}'%(self.file_name,self.is_root,len(self.cases),len(self.suites),str(self.problem_size))
        return _buffer_

    MODULE_COUNTER = 1
    
    def read(self,path,handler,traverse_suites):
        self.file_name = os.path.join(path,suite_info.BASE_FILE_NAME)
        if not os.path.exists(self.file_name):
            handler.error("Unable to find file '%s'."%(self.file_name))
            return False
        module = None
        try:
            module = imp.load_source("_temp_%d"%(suite_info.MODULE_COUNTER),self.file_name)
            suite_info.MODULE_COUNTER += 1
            self.__read_name__(module,handler)
            self.__read_default_problem_size__(module,handler)
            self.__read_description__(module,handler)
            self.__read_is_root__(module,handler)
            self.__read_problem_size__(module,handler)
            self.__read_suites__(module,handler)
            self.__read_cases__(module,handler)
        except Exception, e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            message  = "\n\tFile:%s\n"%(self.file_name)
            message += "\tLine number:%d\n"%(exc_tb.tb_lineno)
            message += "\t%s\n"%(str(exc_obj))
            handler.error(message)
            return False

        if traverse_suites:
            for suite_name in self.suites:
                suite_path = os.path.join(os.path.dirname(self.file_name),suite_name)
                _s_info_ = suite_info()
                if not _s_info_.read(suite_path,handler,True):
                    handler.error("Unable to resolve suite '%s' in file '%s'."%(suite_name,self.file_name))
                    return False
                self.suite_map[suite_name] = _s_info_
                _s_info_.parent = self
                pass
            pass
        return True
