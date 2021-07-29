# -*- coding: utf-8 -*-
"""
Created on Wed Jun 16 09:05:23 2021

@author: rkrit
"""
# Fetching data from sqlite3 database
import sqlite3

# Create a SQL connection to our SQLite database

class PartsAffeData:
    
    def setup_conn(self):
        self.con = sqlite3.connect("D:\FSWEP\intern_projects\performaFoxpro\CF188.sqlite3")
        self.cur = self.con.cursor()
    
    # #Getting the unique report codes
    # report_ind=[]
    # for row in cur.execute('SELECT distinct Report_ind FROM Data_Forms_cf_349s ;'):
    #     report_ind.append(row)
    # print(report_ind)
    
    
    #Get the unique parts affected
    def unique_parts(self):
        parts_affe=[]
        self.setup_conn()
        for row in self.cur.execute('SELECT distinct Parts_affe FROM Data_Forms_cf_349s ;'):
            print(row)
            parts_affe.append(row)
            print(parts_affe)
            return parts_affe
            
        # 100 nodes
    
    # for row in cur.execute('SELECT adp_contro,Parts_affe,report_ind FROM Data_Forms_cf_349s limit 50;'):
    #     print(row)
    
    # Closing the connection
    def close_conn(self):
        self.con.close()



# Connecting to neo4j and create nodes

from neo4j import GraphDatabase

class PartsNode:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_WO_details(self, message):
        with self.driver.session() as session:
            info = session.write_transaction(self._create_and_return_wo, message)
            print(info)

    @staticmethod
    def _create_and_return_wo(tx, name):
        result = tx.run("CREATE (a:WorkOrder) "
                        "SET a.name = $name "
                        "SET a.title= $name "
                        "RETURN a.name + ', from node ' + id(a)", name=name)
        return result.single()[0]


if __name__ == "__main__":
    data=PartsAffeData()
    parts=data.unique_parts()
    print(parts)
    WO_info = PartsNode("bolt://localhost:7687", "neo4j", "test123")
    for i in range(0,10):
        name="WO"+str(i)
        WO_info.print_WO_details(name)
        
        
WO_info.close()
data.close_conn()