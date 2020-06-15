

# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 11:41:47 2020

@author: Horizon
"""



import glob
import csv
import sys 
import sqlite3


path = r'C:\Users\Horizon\Desktop\datab' # use your path
all_files = glob.glob(path + "/*.csv")

conn = sqlite3.connect('boringme.db')



print ("Opened database successfully");

cur = conn.cursor()

"""
cur.execute('''CREATE TABLE students (
	Name text ,
	avge  real,
	part  integer ,
	
	school integer ,
    city  integer ,
    year  real );''')

print ("Table students created successfully");


cur.execute('''CREATE TABLE city (
	cityID integer  ,
	city text) ;''')

print ("Table city created successfully");

cur.execute('''CREATE TABLE school (
	schoolID integer  ,
	school text
     );''')

print ("Table school created successfully");

cur.execute('''CREATE TABLE part (
	partID integer  ,
	part text
    );''')

print ("Table part created successfully");



"""
dict_city = {}
dict_school={}
dict_part={}

            
cur.execute("select city,cityID from city")         
for row in cur:
    dict_city[row[0]]=row[1]
     
 #------------------------------
cur.execute("select part,partID from part")
for row in cur:
    dict_part[row[0]]=row[1]
      
#--------------------------------   
cur.execute("select school,schoolID from school")
for row in cur:
    dict_school[row[0]]=row[1]

     
#--------------------------------  
stopCounter=0

cur.execute("delete from students")


for filename in all_files:
   with open(filename, encoding='utf-8') as csvfile:                                                     
       reader = csv.reader(csvfile)
       
       for row in reader:
           
           #to_db=[row[0],row[1],dict_part.keys(),dict_school.keys(),dict_city.keys(),row[5]]
           
           if not row[3] in dict_school.keys():
              #print(dict_school.keys()[dict_school.values().index(schoolID)])
               dictsize =  len(dict_school)
               dict_school[row[3]]= dictsize
               to_schooldb=[dictsize,row[3]]
               #print(to_schooldb)
                
               cur.execute("INSERT INTO school(schoolID,school) VALUES (? ,?);" , to_schooldb)
               #conn.commit()  
                 
           if not row[4] in dict_city.keys():
                 dictsize = len(dict_city)
                 dict_city[row[4] ]=dictsize
                 to_citydb=[dictsize,row[4]]
                 #print(to_citydb)
            
                 cur.execute("INSERT INTO city (cityID,city) VALUES (? ,?);" , to_citydb)
                 #conn.commit()
                    
           if not row[2] in dict_part.keys():
                 dictsize = len(dict_part)
                 dict_part[ row[2]]=dictsize
                 to_partdb=[dictsize,row[2] ]
                 #print(dict_part.get(count1))
                
                 cur.execute("INSERT INTO part (partID,part) VALUES (? ,?);" , to_partdb)
                 #conn.commit()     
      
          
           partId=dict_part.get(row[2])
           schoolId=dict_school.get(row[3])
           cityId=dict_city.get(row[4])
           cur.execute("INSERT INTO students VALUES ( ?, ? ,? ,? ,? ,?) ",[row[0] ,row[1] , partId , schoolId ,cityId ,row[5]])
           
"""           if stopCounter>100:
               break
           stopCounter=stopCounter+1"""

conn.commit()
           
   
	       

      
           
           
           








