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
        self.add_argument("target","str",os.path.join("db","master.db"),"Target database")
        pass

    def merge_platforms(self):
        source_platform = self.sourceDB.fetch_using_generic(db_model.platform)
        target_platform = self.targetDB.fetch_using_generic(db_model.platform)
        self.source_platform_map = {}
        self.target_platform_map = {}
        for i in source_platform:
            self.source_platform_map[i.name] = i
            pass
        for i in target_platform:
            self.target_platform_map[i.name] = i
            pass
        for i in self.source_platform_map:
            obj = self.source_platform_map[i]
            if not self.target_platform_map.has_key(i):
                print "Adding platform:",obj.name,"type:",obj.type
                platform_object = self.targetDB.create_unique_object(db_model.platform,"name",obj.name,type=obj.type)
                pass
            else:
                print "Existing platform:",obj.name,"type:",obj.type
                pass
            pass
        return True

    def merge_index_types(self):
        source = self.sourceDB.fetch_using_generic(db_model.index_type)
        target = self.targetDB.fetch_using_generic(db_model.index_type)
        self.source_index_type_map = {}
        self.target_index_type_map = {}
        for i in source:
            self.source_index_type_map[i.name] = i
            pass
        for i in target:
            self.target_index_type_map[i.name] = i
            pass
        for i in self.source_index_type_map:
            obj = self.source_index_type_map[i]
            if not self.target_index_type_map.has_key(i):
                print "Adding index type:",obj.name
                index_object = self.targetDB.create_unique_object(db_model.index_type,"name",obj.name)
                pass
            else:
                print "Existing index type:",obj.name
                pass
            pass
        return True

    def merge_description_objects(self,obj_type,name):
        source = self.sourceDB.fetch_using_generic(obj_type)
        target = self.targetDB.fetch_using_generic(obj_type)
        source_map = {}
        target_map = {}
        for i in source:
            source_map[i.name] = i
            pass
        for i in target:
            target_map[i.name] = i
            pass
        for i in source_map:
            obj = source_map[i]
            if not target_map.has_key(i):
                object = self.targetDB.create_unique_object(obj_type,"name",obj.name,description=obj.description)
                print "Adding ",name,obj.name,obj.description," ID : ",object.id
                target_map[object.name] = object
                pass
            else:
                print "Existing ",name,obj.name,obj.description
                pass
            pass
        return (source_map,target_map)

    def merge_engine(self):
        (self.source_engine_map,self.target_engine_map) = self.merge_description_objects(db_model.engine,"engine")
        return True

    def merge_config(self):
        (self.source_config_map,self.target_config_map) = self.merge_description_objects(db_model.config,"config")
        return True

    def merge_case_type(self):
        (self.source_case_type_map,self.target_case_type_map) = self.merge_description_objects(db_model.case_type,"case_type")
        return True

    def merge_suite(self):
        source = self.sourceDB.fetch_using_generic(db_model.suite)
        target = self.targetDB.fetch_using_generic(db_model.suite)
        self.source_suite_map = {}
        self.target_suite_map = {}
        for i in source:
            self.source_suite_map[i.path] = i
            pass
        for i in target:
            self.target_suite_map[i.path] = i
            pass
        addedSuites = []
        addedSuiteMap = {}
        for i in self.source_suite_map:
            obj = self.source_suite_map[i]
            if not self.target_suite_map.has_key(i):
              
                #print self.target_suite_map
                #_suite_id = self.target_suite_map[suite_object[0].name].id
                suite_object = self.targetDB.create_unique_object(db_model.suite,
                                                                  "path",obj.path,
                                                                  name=obj.name,
                                                                  description=obj.description,
                                                                  parent=obj.parent
                                                                  )
                self.target_suite_map[suite_object.path] = suite_object
                print "Adding suite:",obj.path,obj.name
                if obj.parent != -1:
                    source_object = self.sourceDB.fetch_using(db_model.suite,"id",obj.parent)
                    assert len(source_object) == 1
                    addedSuiteMap[obj.parent] = source_object[0].path
                    addedSuites.append(suite_object)
                pass
            else:
                print "Existing suite:",obj.name
                pass
            pass
        for i in addedSuites:
            target_suite = self.target_suite_map[addedSuiteMap[i.parent]]
            i.parent = target_suite.id 
            pass
        return True

    def merge_case(self):
        source = self.sourceDB.fetch_using_generic(db_model.case)
        target = self.targetDB.fetch_using_generic(db_model.case)
        self.source_case_map = {}
        self.target_case_map = {}
        for i in source:
            self.source_case_map[i.path] = i
            pass
        for i in target:
            self.target_case_map[i.path] = i
            pass
        for i in self.source_case_map:
            obj = self.source_case_map[i]
            if not self.target_case_map.has_key(i):
                print "Adding case:",obj.path,obj.case_type_id,obj.parent
                source_case_data_type_object = self.sourceDB.fetch_using(db_model.case_type,"id",obj.case_type_id)
                assert (source_case_data_type_object != None) and len(source_case_data_type_object)
                _case_type_id = self.target_case_type_map[source_case_data_type_object[0].name].id

                suite_object = self.sourceDB.fetch_using(db_model.suite,"id",obj.parent)
                assert (suite_object != None) and len(suite_object)
                _suite_id = self.target_suite_map[suite_object[0].path].id
                suite_object = self.targetDB.create_unique_object(db_model.case,
                                                                  "path",obj.path,
                                                                  name=obj.name,
                                                                  description=obj.description,
                                                                  path=obj.path,
                                                                  parent=_suite_id,
                                                                  case_type_id=_case_type_id,
                                                                  table_view=obj.table_view,
                                                                  plot_view=obj.plot_view,
                                                                  data=obj.data,
                                                                  )
                pass
            else:
                print "Existing case:",obj.name
                pass
            pass
        return True
            
    def operate(self):
        if operations.operation.operate(self):
            target = self.getSingleOption("target")
            source = self.getSingleOption("name")
            self.targetDB = db.db(target)
            self.sourceDB = db.db(source)
            if not os.path.exists(self.sourceDB.name):
                self.error("Source database does not exist '{0'".format(self.sourceDB.name))
                return False
            self.targetDB.create_database()
            self.sourceDB.create_database()
            status = self.merge_platforms() and self.merge_index_types() and self.merge_engine() and self.merge_config() and self.merge_case_type()
            status = status and self.merge_suite() and self.merge_case()
            return status
        return True

