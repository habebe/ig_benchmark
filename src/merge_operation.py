import os
import sys
import getopt
import shutil
import math
import datetime
import operations
import imp
import suite_info
import json
import db
import db_model
import build_operation

class operation(operations.operation):
    "Merge platform specific database into the master database."
    def __init__(self):
        operations.operation.__init__(self,"merge")
        self.add_argument("source","str",None,"Source database")
        self.add_argument("target","str","master","Target database")
        pass

    def __read_suite_info__(self,suite_path_name,traverse_suites):
        if os.path.exists(suite_path_name):
            s = suite_info.suite_info()
            if s.read(suite_path_name,self,traverse_suites):
                return s
        return None

    def __find_parent__(self,suite):
        if not suite.is_root:
            parent_path = os.path.dirname(os.path.dirname(suite.file_name))
            pSuite = self.__read_suite_info__(parent_path,False)
            if not pSuite:
                self.error("Unable to find root suite in path '%s'."%(parent_path))
                return None
            else:
                suite.parent = pSuite
                pSuite.suite_map[suite.get_name()] = suite
                return self.__find_parent__(pSuite)
        return suite
                

    def __update_suite_info_database__(self,_suite_info_,parent_suite_id=-1):
        suites = self.db.fetch_using_generic(db_model.suite,
                                             name=_suite_info_.get_name(),
                                             parent=parent_suite_id
                                             )
        assert (len(suites) <= 1)
        suite = None
        if (len(suites) == 0):
            suite = self.db.create_unique_object(db_model.suite,
                                                 "name",_suite_info_.get_name(),
                                                 parent=parent_suite_id
                                                 )
        else:
            suite = suites[0]
            pass
        suite.path = _suite_info_.get_path()
        suite.problem_size = json.dumps(_suite_info_.problem_size)
        suite.default_problem_size = _suite_info_.default_problem_size
        suite.description = _suite_info_.description
        self.db.update(suite)
        _suite_info_.id = suite.id
        suite.transient_cases = _suite_info_.cases
        sub_suite_info_list = _suite_info_.suite_map.values()
        for sub_suite_info in sub_suite_info_list:
            self.__update_suite_info_database__(sub_suite_info,suite.id)
        return True

    def __get_case_data__(self,data,property_name,default_value=None):
        if data.has_key(property_name):
            return data[property_name]
        return default_value

    def __update_case_database__(self,_suite_info_,parent_suite=None):
        suites = self.db.fetch_using_generic(db_model.suite,
                                             id=_suite_info_.id
                                             )
        assert (len(suites) == 1)
        suite = suites[0]
        if parent_suite:
            parent_suite.t_sub_suites.append(suite)
            pass
        size = 0
        for case_info in _suite_info_.cases:
            _name = self.__get_case_data__(case_info,"name",None)
            if _name == None:
                _name = self.problem_size
                pass
            
                
            _description = self.__get_case_data__(case_info,"description",None)
            _type = self.__get_case_data__(case_info,"type",None)
            _data = self.__get_case_data__(case_info,"data",None)
            _table_view = self.__get_case_data__(case_info,"table_view",None)
            _plot_view = self.__get_case_data__(case_info,"plot_view",None)
            _operation_object_ = operations._operation_(_type)
            if _operation_object_:
                if _operation_object_.is_runnable():
                    case_type_object = _operation_object_.operation_update_database(self.db)
                    case_objects =  self.db.fetch_using_generic(db_model.case,
                                                                name=_name,
                                                                parent=_suite_info_.id,
                                                                )
                    assert (len(case_objects) <= 1)
                    case_object = None
                    if len(case_objects) == 0:
                        case_object = self.db.create_object(db_model.case,
                                                            name=_name,
                                                            parent=suite.id)
                    else:
                        case_object = case_objects[0]
                        pass
                    case_object.path = _suite_info_.get_path()
                    case_object.description = _description
                    case_object.data = str(_data)
                    case_object.setCaseType(case_type_object)
                    case_object.t_operation = _operation_object_
                    if _table_view:
                        case_object.table_view = json.dumps(_table_view)
                    else:
                        case_object.table_view = None
                    if _plot_view:
                        case_object.plot_view = json.dumps(_plot_view)
                    else:
                        case_object.plot_view = None
                        pass
                    self.db.update(case_object)
                    suite.t_cases.append(case_object)
                    size += 1
                    pass
                else:
                    self.warn("Operation '%s' is not runnable.\nCase:'%s' in suite '%s'.\nIgnoring it.\n"%(
                        _type,_name,suite.name
                        ))
            else:
                self.warn("Unable to find operation '%s' .\nCase:'%s' in suite '%s'.\nIgnoring it.\n"%(
                    _type,_name,suite.name
                    ))
                pass
                    
        sub_suite_info_list = _suite_info_.suite_map.values()
      
        for sub_suite_info in sub_suite_info_list:
            (s,_size) = self.__update_case_database__(sub_suite_info,suite)
            size += _size
            pass
        return (suite,size)
        
    def setup(self):
        if self.dbname == None:
            self.db = db.db()
        else:
            self.db = db.db(self.dbname)
            pass
        self.db.create_database()
        self.suite_name = "."
        if self.arguments and len(self.arguments):
            self.suite_name = self.arguments[0]
        self.suite_name = os.path.abspath(self.suite_name)
        self.suite_info = self.__read_suite_info__(self.suite_name,True)
        if self.suite_info:
            root_suite = self.__find_parent__(self.suite_info)
            db_model.suite.RootSuite = root_suite
            if root_suite:
                if self.__update_suite_info_database__(root_suite):
                    return self.__update_case_database__(self.suite_info)
        return (None,None)
            
    def operate(self):
        if operations.operation.operate(self):
            self.dbname = self.getSingleOption("name")
            self.tag = self.getSingleOption("tag")
            self.update_only = self.hasOption("update")
            (suite_object,size) = self.setup()
            if db_model.suite.RootSuite and suite_object and (not self.update_only):
                rootPath = os.path.dirname(db_model.suite.RootSuite.get_path())
                #build = build_operation.operation()
                #build.parse(["--root","{0}".format(rootPath)])
                #build.operate()
                return suite_object.run(self.db,tag=self.tag,verbose=0)
            return False
        return True

