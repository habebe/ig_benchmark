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
        self.add_argument("root","str",".","root path")
        self.add_argument("master","str",None,"Master database")
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
                #print "Adding platform:",obj.name,"type:",obj.type
                platform_object = self.targetDB.create_unique_object(db_model.platform,"name",obj.name,type=obj.type)
                self.target_platform_map[platform_object.name] = platform_object
                pass
            else:
                #print "Existing platform:",obj.name,"type:",obj.type
                pass
            pass
        self.platform_id_map = {}
        for i in self.source_platform_map:
            _source = self.source_platform_map[i]
            _target = self.target_platform_map[i]
            self.platform_id_map[_source.id] = _target.id
            pass
        return True

    def merge_tag(self):
        source = self.sourceDB.fetch_using_generic(db_model.tag)
        target = self.targetDB.fetch_using_generic(db_model.tag)
        self.source_tag_map = {}
        self.target_tag_map = {}
        for i in source:
            self.source_tag_map[i.name] = i
            pass
        for i in target:
            self.target_tag_map[i.name] = i
            pass
        for i in self.source_tag_map:
            obj = self.source_tag_map[i]
            if not self.target_tag_map.has_key(i):
                #print "Adding tag:",obj.name,"type:",obj.timestamp
                tag_object = self.targetDB.create_unique_object(db_model.tag,"name",obj.name,timestamp=obj.timestamp)
                self.target_tag_map[tag_object.name] = tag_object
                pass
            else:
                #print "Existing tag:",obj.name,"type:",obj.timestamp
                pass
            pass
        self.tag_id_map = {}
        for i in self.source_tag_map:
            _source = self.source_tag_map[i]
            _target = self.target_tag_map[i]
            self.tag_id_map[_source.id] = _target.id
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
                #print "Adding index type:",obj.name
                index_object = self.targetDB.create_unique_object(db_model.index_type,"name",obj.name)
                self.target_index_type_map[obj.name] = index_object
                pass
            else:
                #print "Existing index type:",obj.name
                pass
            pass
        self.index_type_id_map = {}
        for i in self.source_index_type_map:
            _source = self.source_index_type_map[i]
            _target = self.target_index_type_map[i]
            self.index_type_id_map[_source.id] = _target.id
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
                #print "Adding ",name,obj.name,obj.description," ID : ",object.id
                target_map[object.name] = object
                pass
            else:
                #print "Existing ",name,obj.name,obj.description
                pass
            pass
        source_target_id_map = {}
        for i in source_map:
            _source = source_map[i]
            _target = target_map[i]
            source_target_id_map[_source.id] = _target.id
            pass
        return (source_map,target_map,source_target_id_map)

    def merge_engine(self):
        (self.source_engine_map,self.target_engine_map,self.engine_id_map) = self.merge_description_objects(db_model.engine,"engine")
        return True

    def merge_config(self):
        (self.source_config_map,self.target_config_map,self.config_id_map) = self.merge_description_objects(db_model.config,"config")
        return True

    def merge_process_description(self):
        (self.source_process_description_map,self.target_process_description_map,self.process_description_id_map) = self.merge_description_objects(db_model.process_description,"process_description")
        return True

    def merge_case_type(self):
        (self.source_case_type_map,self.target_case_type_map,self.case_type_id_map) = self.merge_description_objects(db_model.case_type,"case_type")
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
                #print "Adding suite:",obj.path,obj.name
                if obj.parent != -1:
                    source_object = self.sourceDB.fetch_using(db_model.suite,"id",obj.parent)
                    assert len(source_object) == 1
                    addedSuiteMap[obj.parent] = source_object[0].path
                    addedSuites.append(suite_object)
                pass
            else:
                #print "Existing suite:",obj.name
                pass
            pass
        for i in addedSuites:
            target_suite = self.target_suite_map[addedSuiteMap[i.parent]]
            i.parent = target_suite.id
            self.targetDB.update(i)
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
                #print "Adding case:",obj.path,obj.case_type_id,obj.parent
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
                self.target_case_map[obj.path] = suite_object
                pass
            else:
                #print "Existing case:",obj.name
                pass
            pass
        self.case_id_map = {}
        for i in self.source_case_map:
            _source = self.source_case_map[i]
            _target = self.target_case_map[i]
            self.case_id_map[_source.id] = _target.id
            pass
        return True
    
    def merge_case_data_stat(self):
        source = self.sourceDB.fetch_using_generic(db_model.case_data_stat)
        target = self.targetDB.fetch_using_generic(db_model.case_data_stat)
        platform_ids = {}
        for i in self.source_platform_map:
            target_platform = self.target_platform_map[i]
            platform_ids[target_platform.id] = i
            pass
        for i in target:
            platform_id = i.platform_id()
            if platform_ids.has_key(platform_id):
                #print "Deleting Case Stat ID:",i.id,"from platform",platform_id,"name:",platform_ids[platform_id]
                self.targetDB.delete(db_model.case_data_stat,i.id)
                pass
            pass
        for i in source:
            engine   = i.engine_id()
            platform = i.platform_id()
            index    = i.index_type_id()
            config   = i.config_id()
            process_description = i.process_description_id()
            
            data = json.loads(i.key)
            data[0]  = self.case_id_map[data[0]]
            data[1]  = self.engine_id_map[engine]
            data[8]  = self.platform_id_map[platform]
            data[11] = self.index_type_id_map[index]
            data[12] = self.config_id_map[config]
            data[13] = self.process_description_id_map[process_description]
            #print "Adding Case Stat ID:",i.id
            case_data_stat_object = self.targetDB.create_unique_object(db_model.case_data_stat,
                                                                       "key",json.dumps(data),
                                                                       case_id=data[0]
                                                                       )
            case_data_stat_object.case_id = data[0]
            case_data_stat_object.key = json.dumps(data)
            case_data_stat_object.counter = i.counter
            
            case_data_stat_object.time_min = i.time_min
            case_data_stat_object.time_max = i.time_max
            case_data_stat_object.time_sum = i.time_sum
            
            case_data_stat_object.rate_min = i.rate_min
            case_data_stat_object.rate_max = i.rate_max
            case_data_stat_object.rate_sum = i.rate_sum
            
            case_data_stat_object.memory_init_min = i.memory_init_min
            case_data_stat_object.memory_init_max = i.memory_init_max
            case_data_stat_object.memory_init_sum = i.memory_init_sum
                        
            case_data_stat_object.memory_used_min = i.memory_used_min
            case_data_stat_object.memory_used_max = i.memory_used_max
            case_data_stat_object.memory_used_sum = i.memory_used_sum
            
            case_data_stat_object.memory_committed_min = i.memory_committed_min
            case_data_stat_object.memory_committed_max = i.memory_committed_max
            case_data_stat_object.memory_committed_sum = i.memory_committed_sum
            
            case_data_stat_object.memory_max_min = i.memory_max_min
            case_data_stat_object.memory_max_max = i.memory_max_max
            case_data_stat_object.memory_max_sum = i.memory_max_sum
            self.targetDB.update(case_data_stat_object)
            pass
        
        return True

    def merge_case_data(self):
        source = self.sourceDB.fetch_using_generic(db_model.case_data)
        target = self.targetDB.fetch_using_generic(db_model.case_data)
        platform_ids = {}
        for i in self.source_platform_map:
            target_platform = self.target_platform_map[i]
            platform_ids[target_platform.id] = i
            pass
        counter = 1
        for i in target:
            platform_id = i.platform_id
            if platform_ids.has_key(platform_id):
                #print "[{0}] Deleting Case ID:".format(counter),i.id,"from platform",platform_id,"name:",platform_ids[platform_id]
                self.targetDB.delete(db_model.case_data,i.id)
                counter += 1
                pass
            pass
        for i in source:
            case_data_object = self.targetDB.create_object(db_model.case_data,
                                                           timestamp=i.timestamp,
                                                           case_id=self.case_id_map[i.case_id],
                                                           engine_id=self.engine_id_map[i.engine_id],
                                                           tag_id=self.tag_id_map[i.tag_id],
                                                           size=i.size,
                                                           time=i.time,
                                                           memory_init=i.memory_init,
                                                           memory_used=i.memory_used,
                                                           memory_committed=i.memory_committed,
                                                           memory_max=i.memory_max,
                                                           rate=i.rate,
                                                           page_size=i.page_size,
                                                           cache_init=i.cache_init,
                                                           cache_max=i.cache_max,
                                                           tx_size=i.tx_size,
                                                           platform_id=self.platform_id_map[i.platform_id],
                                                           threads=i.threads,
                                                           index_id=self.index_type_id_map[i.index_id],
                                                           config_id=self.config_id_map[i.config_id],
                                                           status=i.status,
                                                           op_size=i.op_size,
                                                           processes=i.processes,
                                                           process_description_id=self.process_description_id_map[i.process_description_id]
                                                           )
            pass
        return True

    def collect_databases(self,path):
        path = os.path.abspath(path)
        db_files = []
        listing = os.listdir(path)
        for i in listing:
            if i.endswith(".db"):
                if i != "master.db":
                    db_files.append(os.path.join(path,i))
                    pass
                pass
            pass
        return db_files
    
    def operate(self):
        if operations.operation.operate(self):
            root = self.getSingleOption("root")
            target = self.getSingleOption("target")
            path = os.path.abspath(os.path.join(root,"db"))
            sources =  self.collect_databases(path)
            if target == None:
                self.targetDB = db.db(os.path.join(path,"master.db"))
            else:
                self.targetDB = db.db(target)
                pass
            self.targetDB.create_database()
            for source in sources:
                self.sourceDB = db.db(source)
                self.sourceDB.create_database()
                status = self.merge_tag() and self.merge_platforms() and self.merge_process_description() and self.merge_index_types() and self.merge_engine() and self.merge_config() and self.merge_case_type()
                status = status and self.merge_suite() and self.merge_case()
                status = status and self.merge_case_data()
                status = status and self.merge_case_data_stat()
                pass
            return True
        return True

