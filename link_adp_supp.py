# -*- coding: utf-8 -*-
"""
Created on Thu Jul 15 17:32:56 2021

@author: rkrit

Data_Forms_cf_349s
Supplement/Supp_data2

Supp_data2 : DD - Maintenance action deferred
Script to link supp_data2 of adp nodes to their parent SUPP codes
Maximum length of supp_code is 9

Supp_code TLIR not found to be matching in 2019 records
"""

from neo4j import GraphDatabase
import sqlite3
import re


driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test123"))

# Types of possible parent supp codes
SUPP_CODE=['6','AB','ACC','AETE','ASI','A','B','CAMP','XXCD','XXCF','CON',
           'CONFIG','COR','DD','DEA','DEV','DI','DRED','XXXXXXFS','IHM','INT',
           'L','NDH','NDT','XXNS','NSR','OPIMP','OPREM','OSI','PER','PRI',
           'PROTOTYPE','PSI','QAI','RAMP','REO','ROB','SI','STRESS','SUP',
           'TESTCELL','TLM','TLRO','WEEK']

# Splitting the incoming variable into the prefix(xx) and the pattern part
def getPattern(var):  
    index=var.rfind('X')
    prefix=var[:index+1]
    suffix=var[index+1:]  
    return prefix,suffix,index+1

# Check if the variable ends with the given string for XX case
def checkPattern(var,string):  
    pattern = "[A-Z|a-z|0-9]*"+ string+"$"
    if re.match(pattern,var):
        return True
    elif string in var:
        return True
    else:
        return False


# Returns child and parent of the given supp code
def matchSuppCodes(input):
    global SUPP_CODE
    for i in SUPP_CODE:
        if "XX" not in i:        
            pattern="^"+i
            if re.match(pattern,input):              
                return input,i
          
        else:        
            prefix,pattern,index=getPattern(i)         
            if checkPattern(input,pattern):
               # print("check pattern true")
               child=input
               parent=i
               return child,parent
    return False,False 


# Function to get adp and its supp code for a given time frame
def supp_adp_info():
    try:
        con=sqlite3.connect("D:\FSWEP\intern_projects\performaFoxpro\CF188.sqlite3")
        cur=con.cursor()
        output=[]  
        pattern='2019%'
        cur.execute('SELECT  adp_contro,supp_data2 FROM Data_Forms_cf_349p where creation_t like ? ' ,(pattern,))
        output=cur.fetchall()   
        adp_contro = [i[0] for i in output]
        suppdata2= [i[1] for i in output]     
        return adp_contro,suppdata2
        
    finally:
        if (con):
            con.close() 
    



# Code to link adp,supp with parent SUP            
class link_adp_supp:
    def __init__(self):
        self.driver = driver
    
    def close(self):
        self.driver.close()

    def print_part_details(self, adp,supp,property):
        with self.driver.session() as session:
            info = session.write_transaction(self.link_nodes, adp,supp,property)
            print(info)

    @staticmethod
    def link_nodes(tx, node1,node2,parent):
        result =  tx.run("MATCH (a:ADP {id: $adp}), "
            " (b:SUPP {id:$sup})"
           "MERGE (a)-[:parent{supp_code2:$parent}]->(b)",
           adp=node1, sup=node2,parent=parent)
        return result.single() 

  

if __name__ == "__main__":
    
    
     with driver.session() as session:     
    # Link adp and supp nodes together
        adpSupp=link_adp_supp()
        adp,suppCode=supp_adp_info()
        for i in range(0,len(adp)):
            if (suppCode[i]!="" and adp[i]!=""):             
                child,parent=matchSuppCodes(suppCode[i])               
                if child!=False:                    
                    adpSupp.print_part_details(adp[i],suppCode[i],parent)
             
      
        adpSupp.close()
    
