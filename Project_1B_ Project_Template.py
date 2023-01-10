#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(root, dirs, files)
    print(file_path_list)


# In[3]:


file_path_list[0:1]


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[4]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line)
            
# uncomment the code below if you would like to get total number of rows 
print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue # skip artist name is null row
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[5]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[6]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[7]:


# TO-DO: Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """)
except Exception as e:
    print(e)


# #### Set Keyspace

# In[8]:


# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[9]:


## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4
query_1 = "select artist, song, length from event_data where sessionId = 338 and itemInSession = 4"

                    


# In[13]:


# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

#create event_data table
query = "CREATE TABLE IF NOT EXISTS event_data"
query = query + "(artist varchar, firstName varchar, gender varchar, itemInSession int, lastName varchar, length decimal, level varchar, location varchar, sessionId int, song varchar, userId int, PRIMARY KEY(sessionId, itemInSession, userId))"
try:
    session.execute(query)
except Exception as e:
    print(e)
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        #print(line)
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO event_data (artist, firstName, gender, itemInSession, lastName, length,level, location, sessionId, song, userId)"
        query = query + "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        #session.execute(query)
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        
        session.execute(query, (line[0], line[1], line[2], int(line[3]), line[4], float(line[5]), line[6], line[7], int(line[8]), line[9], int(line[10])))
        


# #### Do a SELECT to verify that the data have been inserted into each table

# In[11]:


## TO-DO: Add in the SELECT statement to verify the data was entered into the table
results = session.execute(query_1)
for result in results:
    print('artist_name: ',result.artist, ', song: ', result.song, ', length: ', result.length)


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[14]:


#create event_data table for query 2
query = "CREATE TABLE IF NOT EXISTS event_data_2"
query = query + "(artist varchar, firstName varchar, gender varchar, itemInSession int, lastName varchar, length decimal, level varchar, location varchar, sessionId int, song varchar, userId int, PRIMARY KEY(sessionId, userId, itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        print(line)
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO event_data_2 (artist, firstName, gender, itemInSession, lastName, length,level, location, sessionId, song, userId)"
        query = query + "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        #session.execute(query)
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        
        session.execute(query, (line[0], line[1], line[2], int(line[3]), line[4], float(line[5]), line[6], line[7], int(line[8]), line[9], int(line[10])))
        


# In[23]:


## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
query_2 = "select artist, song , lastName , firstName from event_data_2 where sessionId = 182 and userId = 10"
results = session.execute(query_2)
for result in results:
    print('artist_name: ',result.artist, ', song: ', result.song, 'user_name: ', result.lastname+' '+result.firstname)
                    


# In[27]:


#create event_data_3 table for query 3
query = "CREATE TABLE IF NOT EXISTS event_data_3"
query = query + "(artist varchar, firstName varchar, gender varchar, itemInSession int, lastName varchar, length decimal, level varchar, location varchar, sessionId int, song varchar, userId int, PRIMARY KEY(song,sessionId,itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        print(line)
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO event_data_3 (artist, firstName, gender, itemInSession, lastName, length,level, location, sessionId, song, userId)"
        query = query + "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        #session.execute(query)
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        
        session.execute(query, (line[0], line[1], line[2], int(line[3]), line[4], float(line[5]), line[6], line[7], int(line[8]), line[9], int(line[10])))
        


# In[28]:


## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
query_3 = "select lastname , firstname from event_data_3 where song = 'All Hands Against His Own'"
results = session.execute(query_3)
for result in results:
    print('user_name: ', result.lastname+' '+result.firstname)

                    


# ### Drop the tables before closing out the sessions

# In[4]:


## TO-DO: Drop the table before closing out the sessions


# In[ ]:


session.execute('drop table event_data_3 ')


# ### Close the session and cluster connection¶

# In[ ]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




