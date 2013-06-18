import sqlite3
import sys
import getopt
import os
import shutil
import db_model
import datetime
import types
import json
import imp
from db_types import *
import platform
import socket

class db:
    #default_name = platform.uname()[0].lower()
    default_name = socket.gethostname().lower()
    schema_classes = [
        db_model.engine,
        db_model.os_type,
        db_model.case_type,
        db_model.platform,
        db_model.index_type,
        db_model.tag,
        db_model.suite,
        db_model.case,
        db_model.case_data,
        db_model.case_data_stat
        ]

    def now_string(self,includeSeconds=True):
        if includeSeconds:
            return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    def create_table(self,Class,cursor):
        statement = "CREATE TABLE IF NOT EXISTS %s ("%(Class.get_name())
        counter = len(Class._property_)
        for member in Class._property_:
            _name_ = member[0]
            _type_ = member[1]
            if counter > 1:
                statement += "%s %s,"%(_name_,db_types.NAME_MAP[_type_])
            else:
                statement += "%s %s"%(_name_,db_types.NAME_MAP[_type_])
            counter -= 1
            pass
        statement += ")"
        
        cursor.execute(statement)
        return statement

    def __init__(self,name = default_name):
        self.name = name.lower()
        if not self.name.endswith(".db"):
            self.name += ".db"
            pass
        self.connection = None
        pass

    def exists(self):
        return os.path.exists(self.name)
    
    def create_database(self):
        self.connection = sqlite3.connect(self.name)
        cursor = self.connection.cursor()
        for dbObject in db.schema_classes:
            dbObject.create_table(self,cursor)
        self.connection.commit()
        pass


    def __raw_query__(self,select,table,where):
        self.connection = sqlite3.connect(self.name)
        cursor = self.connection.cursor()
        string = "SELECT %s FROM %s "%(select,table)
        output = sys.stdout
        if where:
            string += " WHERE " + where
            pass
        cursor.execute(string)
        data = cursor.fetchall()
        counter = len(data)
        string = ""
        counter = len(cursor.description)
        description = cursor.description
        for i in cursor.description:
            string += "%s"%(str(i[0]))
            if counter != 1:
                string += "|"
            counter -= 1
        #print >> output,string
        for row in data:
            string = ""
            counter = len(row)
            for i in row:
                string += "%s"%str(i)
                if counter != 1:
                    string += "|"
                counter -= 1
            #print >> output,string
            counter -= 1
            pass
        self.connection.commit()
        return (description,data)

    def raw_query(self,select,table,where):
        result = None
        try:
            result = self.__raw_query__(select,table,where)
        except sqlite3.OperationalError as inst:
            #print >> sys.stderr,"Database error:",
            return (str(inst),result)
        return (None,result)

    def __delete_tag__(self,where):
        self.connection = sqlite3.connect(self.name)
        cursor = self.connection.cursor()
        string = "SELECT id,name FROM %s "%(db_model.tag.get_name())
        if where:
            string += " WHERE %s "%(where)
            pass
        cursor.execute(string)
        data = cursor.fetchall()
       
        counter = len(data)
        string = ""
        output = sys.stdout
        for row in data:
            string = ""
            counter = len(row)
            for i in row:
                string += "%s"%str(i)
                if counter != 1:
                    string += "|"
                counter -= 1
            
            tag_string  = "DELETE FROM %s where id=%d"%(db_model.tag.get_name(),row[0])
            data_string = "DELETE FROM %s where tag_id=%d"%(db_model.case_data.get_name(),row[0])
            #print >> output,"\t",tag_string
            cursor.execute(tag_string)
            #print >> output,"\t",data_string
            cursor.execute(data_string)
            counter -= 1
            pass
        self.connection.commit()
        return data

    def delete_tag(self,where):
        error = None
        items = None
        try:
            items = self.__delete_tag__(where)
        except sqlite3.OperationalError as inst:
            error = str(inst)
            pass
        return (error,items)
    
    def fetch_using(self,Class,property_name,property_value):
        statement = "SELECT * from %s where %s = ?"%(Class.get_name(),property_name)
        cursor = self.connection.cursor()
        cursor.execute(statement,(property_value,))
        data = cursor.fetchall()
        self.connection.commit()
        objects = []
        for i in data:
            pObject = Class()
            pObject.set_data(i)
            objects.append(pObject)
            pass
        return objects

    def fetch_using_generic(self,Class,**kwargs):
        where_statement = 'where '
        counter = len(kwargs)
        values = ()
        for key in kwargs:
            value = kwargs[key]
            where_statement += ' %s = ? '%(key)
            if counter > 1:
                where_statement += " AND "
                pass
            values = values + (value,)
            counter -= 1
            pass
        if len(kwargs) == 0:
            where_statement = ""
            pass
        statement = "SELECT * from %s %s "%(Class.get_name(),where_statement)
        cursor = self.connection.cursor()
        cursor.execute(statement,values)
        data = cursor.fetchall()
        self.connection.commit()
        objects = []
        for i in data:
            pObject = Class()
            pObject.set_data(i)
            objects.append(pObject)
            pass
        
        return objects

    def fetch_single_using(self,Class,property_name,property_value):
        objects = self.fetch_using(Class,property_name,property_value)
        if len(objects):
            return objects[0]
        return None

    def update(self,pObject):
        properties = ""
        _property_ = pObject._property_[1:]
        counter = len(_property_)
        values = ()
        for p in _property_:
            properties += "%s=?"%(p[0])
            if counter > 1:
                properties += ","
            counter -= 1
            pass
        values = pObject.get_data(False)
        values += (pObject.id,)
        statement = "UPDATE %s SET %s WHERE id=?"%(pObject.get_name(),properties)
        cursor = self.connection.cursor()
        cursor.execute(statement,values)
        self.connection.commit()

    def __insert__(self,Class,properties):
        property_names  = ""
        property_values = ()
        counter = len(properties)
        value_string = ""
        for p in properties:
            key = p[0]
            value = p[1]
            property_names += "%s"%(key)
            value_string += "?"
            if counter > 1:
                property_names += ","
                value_string += ","
                pass
            counter -= 1
            property_values += (value,)
            pass
        statement = "INSERT INTO %s (%s) VALUES(%s)"%(Class.get_name(),property_names,value_string)
        cursor = self.connection.cursor()
        #print statement
        #print property_values
        cursor.execute(statement,property_values)
        rowid = cursor.lastrowid
        self.connection.commit()
        return rowid

    def create_unique_object(self,Class,unique_name,unique_value,**kwargs):
        properties = [(unique_name,unique_value)]
        for key in kwargs:
            properties.append((key,kwargs[key]))
            pass
        
        pObject = self.fetch_single_using(Class,unique_name,unique_value)
        if pObject == None:
            self.__insert__(Class,properties)
        pObject = self.fetch_single_using(Class,unique_name,unique_value)
        return pObject


    def create_object(self,Class,**kwargs):
        properties = []
        for key in kwargs:
            properties.append((key,kwargs[key]))
            pass
        _id = self.__insert__(Class,properties)
        pObject = self.fetch_single_using(Class,"id",_id)
        return pObject
        

