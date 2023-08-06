#Lukasz Grzybek


#http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt

#

#Part 1



user = """
CREATE TABLE User
(
  id Number,
  name VARCHAR2(30),
  screen_name VARCHAR2(30),
  description VARCHAR2(30),
  friends_count VARCHAR2(30),

  CONSTRAINT userid_PK
    Primary KEY(id)



);
"""
geo = """
CREATE TABLE Geo
(
  
  Longitude VARCHAR2(30),
  Latitude  VARCHAR2(30),
  Type VARCHAR2(30),
  



  CONSTRAINT geoid_PK
    Primary KEY(Longitude,Latitude)
    


);
"""

twitter = """
CREATE TABLE Twitter
(
  created_at VARCHAR2(30),
  id_str VARCHAR2(30),
  text VARCHAR2(30),
  source VARCHAR2(30),
  in_reply_to_user_id VARCHAR2(30),
  in_reply_to_screen_name VARCHAR2(30),
  in_reply_to_status_id VARCHAR2(30),
  retweet_count VARCHAR2(30),
  contributors VARCHAR2(30),
  user_id Number,
  Long VARCHAR2(30),
  Lat  VARCHAR2(30),

CONSTRAINT geoid_PK
    Primary KEY(id_str),


  CONSTRAINT user_FK
    FOREIGN KEY(user_id)
    References user(id),

  CONSTRAINT geo_FK
    FOREIGN KEY(Long,Lat)
    References Geo(Longitude,Latitude)



);
"""


import urllib.request
import json
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import re

import sqlite3

conn = sqlite3.connect('dsc450.db') 
cursor = conn.cursor()


drop1 = 'Drop Table User'
cursor.execute(drop1)
drop2 = 'Drop Table Geo'
cursor.execute(drop2)
drop3 = 'Drop Table Twitter'
cursor.execute(drop3)

cursor.execute(user) 
cursor.execute(geo) 

cursor.execute(twitter) 


#120k tweets

#a Use python to download tweets from the web and save to a local text file (not into a database yet, just to a text file). 

atime120 = np.zeros(120000)
webfile = 'http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt'

alltweets = urllib.request.urlopen(webfile)


acc = 0
new = open('oneday_tweets120k.txt','w',encoding="utf-8")
for i in range(120000):
  current = time.time()
  tweet = alltweets.readline()
  
  new.write(tweet.decode("utf-8"))


  time2 = time.time()
  diff = time2-current
  acc += float(diff)


  atime120[i] += acc

new.close()


print(atime120)


#b. Repeat what you did in part 1-a, but instead of saving tweets to the file, populate the 3-table schema that you previously created in SQLite. 

btime120 = np.zeros(120000)
acc = 0

tweet_valid = []
user_valid = []
geo_type_valid = []
geo_long_valid = []
geo_lat_valid = []
for i in range(120000):
  current = time.time()
  tweet = alltweets.readline()
  tdict = json.loads(tweet.decode('utf8'))
  
  user_valid.append(tdict["user"])
  tweet_valid.append(tdict)
  try:
    geo_type_valid.append(tdict["geo"]['type'])
  except TypeError:
    geo_type_valid.append(None)
  try:
    geo_long_valid.append(tdict["geo"]["coordinates"][0])
  except TypeError:
    geo_long_valid.append(None)
  try:
    geo_lat_valid.append(tdict["geo"]["coordinates"][1])  
  except TypeError:
    geo_lat_valid.append(None) 

  time2 = time.time()
  diff = time2-current
  acc += float(diff)


  btime120[i] += acc

all_acc = []
acc = 0
for u in user_valid:
  current = time.time()
  u1 = u["id_str"]
  u2 = u["name"]
  u3 = u["screen_name"]
  u4 = u["description"]
  u5 = u["friends_count"]
  inserts = (u1,u2,u3,u4,u5)
  try:
    cursor.execute("INSERT INTO User VALUES(?,?,?,?,?);",inserts)
  except sqlite3.IntegrityError:
    pass

  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  all_acc.append(acc)

btime120 = btime120 + all_acc





all_acc = []
acc = 0
for gtype,glong,glat in zip(geo_type_valid,geo_long_valid,geo_lat_valid):
  current = time.time()
  g1 = glong
  g2 = glat
  g3 = gtype

  
  inserts = (g1,g2,g3)
  try:
    cursor.execute("INSERT INTO Geo VALUES(?,?,?);",inserts)
  except sqlite3.IntegrityError:
    pass
  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  all_acc.append(acc)

btime120 = btime120 + all_acc






all_acc = []
acc = 0
for t in tweet_valid:
  current = time.time()
  t1 = t["created_at"]
  t2 = t["id_str"]
  t3 = t["text"]
  t4 = t["source"]
  t5 = t["in_reply_to_user_id"]
  t6 = t["in_reply_to_screen_name"]
  t7 = t["in_reply_to_status_id"]
  t8 = t["retweet_count"]
  t9 = t["contributors"]
  t10 = t["user"]["id_str"]
  try:
    t11 = t['geo']['coordinates'][0]
    t12 = t['geo']['coordinates'][1]
  except TypeError:
    t11 = None
    t12 = None

  inserts = (t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12)
  try:
    cursor.execute("INSERT INTO Twitter VALUES(?,?,?,?,?,?,?,?,?,?,?,?);",inserts)
  except sqlite3.IntegrityError:
    pass
  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  all_acc.append(acc)

btime120 = btime120 + all_acc
  
print(btime120)

user_result = cursor.execute('SELECT count(*) from User')
userread = user_result.fetchone()

print(f"B Number of rows in user table = {userread}")


geo_result = cursor.execute('SELECT count(*) from Geo')
georread = geo_result.fetchone()
print(f"B Number of rows in geo table = {georread}")



twitter_result = cursor.execute('SELECT count(*) from Twitter')
twitteread = twitter_result.fetchone()


print(f"B Number of rows in twitter table = {twitteread}")



#print(f"B - Time reading 120k tweets into database = {b120}")

#c.  Use your locally saved tweet file to repeat the database population step from part-c. That is, load the tweets into the 3-table database using your saved file with tweets.


drop1 = 'Drop Table User'
cursor.execute(drop1)
drop2 = 'Drop Table Geo'
cursor.execute(drop2)
drop3 = 'Drop Table Twitter'
cursor.execute(drop3)

cursor.execute(user) 
cursor.execute(geo) 

cursor.execute(twitter) 






file = open('oneday_tweets120k.txt','r', encoding="utf8")


tweet_valid = []
user_valid = []
geo_type_valid = []
geo_long_valid = []
geo_lat_valid = []


ctime120 = np.zeros(120000)
acc = 0


for i in range(120000):
  current = time.time()
  tweet = file.readline()
  tdict = json.loads(tweet.encode('utf8'))
  
  user_valid.append(tdict["user"])
  tweet_valid.append(tdict)
  try:
    geo_type_valid.append(tdict["geo"]['type'])
  except TypeError:
    geo_type_valid.append(None)
  try:
    geo_long_valid.append(tdict["geo"]["coordinates"][0])
  except TypeError:
    geo_long_valid.append(None)
  try:
    geo_lat_valid.append(tdict["geo"]["coordinates"][1])  
  except TypeError:
    geo_lat_valid.append(None) 

  time2 = time.time()
  diff = time2-current
  acc += float(diff)


  ctime120[i] += acc
  
all_acc = []
acc = 0
for u in user_valid:
  current = time.time()
  u1 = u["id_str"]
  u2 = u["name"]
  u3 = u["screen_name"]
  u4 = u["description"]
  u5 = u["friends_count"]
  inserts = (u1,u2,u3,u4,u5)
  try:
    cursor.execute("INSERT INTO User VALUES(?,?,?,?,?);",inserts)
  except sqlite3.IntegrityError:
    pass

  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  all_acc.append(acc)

ctime120 = ctime120 + all_acc



all_acc = []
acc = 0
for gtype,glong,glat in zip(geo_type_valid,geo_long_valid,geo_lat_valid):
  current = time.time()
  g1 = glong
  g2 = glat
  g3 = gtype

  
  inserts = (g1,g2,g3)
  try:
    cursor.execute("INSERT INTO Geo VALUES(?,?,?);",inserts)
  except sqlite3.IntegrityError:
    pass
  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  all_acc.append(acc)

ctime120 = ctime120 + all_acc  




all_acc = []
acc = 0
for t in tweet_valid:
  current = time.time()
  t1 = t["created_at"]
  t2 = t["id_str"]
  t3 = t["text"]
  t4 = t["source"]
  t5 = t["in_reply_to_user_id"]
  t6 = t["in_reply_to_screen_name"]
  t7 = t["in_reply_to_status_id"]
  t8 = t["retweet_count"]
  t9 = t["contributors"]
  t10 = t["user"]["id_str"]
  try:
    t11 = t['geo']['coordinates'][0]
    t12 = t['geo']['coordinates'][1]
  except TypeError:
    t11 = None
    t12 = None

  inserts = (t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12)
  try:
    cursor.execute("INSERT INTO Twitter VALUES(?,?,?,?,?,?,?,?,?,?,?,?);",inserts)
  except sqlite3.IntegrityError:
    pass

  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  all_acc.append(acc)

ctime120 = ctime120 + all_acc   
print(ctime120)

user_result = cursor.execute('SELECT count(*) from User')
userread = user_result.fetchone()


print(f"C Number of rows in user table = {userread}")

geo_result = cursor.execute('SELECT count(*) from Geo')
georread = geo_result.fetchone()


print(f"C Number of rows in geo table = {georread}")

twitter_result = cursor.execute('SELECT count(*) from Twitter')
twitteread = twitter_result.fetchone()

print(f"C Number of rows in twitter table = {twitteread}")




#d.  Repeat the same step with a batching size of 6000 (i.e. by inserting 6000 rows at a time with executemany instead of doing individual inserts). 
#Since many of the tweets are missing a Geo location, its fine for the batches of Geo inserts to be smaller than 6000. 

drop1 = 'Drop Table User'
cursor.execute(drop1)
drop2 = 'Drop Table Geo'
cursor.execute(drop2)
drop3 = 'Drop Table Twitter'
cursor.execute(drop3)

cursor.execute(user) 
cursor.execute(geo) 

cursor.execute(twitter) 






file = open('oneday_tweets120k.txt','r', encoding="utf8")


tweet_valid = []
user_valid = []
geo_type_valid = []
geo_long_valid = []
geo_lat_valid = []



dtime120 = []
acc = 0
counter = 0

for i in range(120000):
  current = time.time()
  tweet = file.readline()
  tdict = json.loads(tweet.encode('utf8'))
  
  user_valid.append(tdict["user"])
  tweet_valid.append(tdict)
  try:
    geo_type_valid.append(tdict["geo"]['type'])
  except TypeError:
    geo_type_valid.append(None)
  try:
    geo_long_valid.append(tdict["geo"]["coordinates"][0])
  except TypeError:
    geo_long_valid.append(None)
  try:
    geo_lat_valid.append(tdict["geo"]["coordinates"][1])  
  except TypeError:
    geo_lat_valid.append(None) 


  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  counter += 1

  if counter == 6000:
    dtime120.append(acc)
    counter = 0

dtime120 = np.array(dtime120)


all_acc = []
acc = 0

all_u = []
for u in user_valid:
  
  u1 = u["id_str"]
  u2 = u["name"]
  u3 = u["screen_name"]
  u4 = u["description"]
  u5 = u["friends_count"]
  inserts = (u1,u2,u3,u4,u5)
  all_u.append(inserts)


batchnumber = int(120000/6000)
for b in range(batchnumber):
  current = time.time()  
  try:
    cursor.executemany("INSERT INTO User VALUES(?,?,?,?,?);",all_u[(b*6000):((b+1)*6000)])
  except sqlite3.IntegrityError:
    pass
  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  all_acc.append(acc)

dtime120 = dtime120 + all_acc  


all_acc = []
acc = 0
all_g = []
for gtype,glong,glat in zip(geo_type_valid,geo_long_valid,geo_lat_valid):
  g1 = glong
  g2 = glat
  g3 = gtype

  
  inserts = (g1,g2,g3)
  all_g.append(inserts)

for b in range(batchnumber):
  current = time.time()  
  try:
    cursor.executemany("INSERT INTO Geo VALUES(?,?,?);",all_g[(b*6000):((b+1)*6000)])
  except sqlite3.IntegrityError:
    pass
  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  all_acc.append(acc)

dtime120 = dtime120 + all_acc  

all_acc = []
acc = 0  
all_t = []
for t in tweet_valid:
  t1 = t["created_at"]
  t2 = t["id_str"]
  t3 = t["text"]
  t4 = t["source"]
  t5 = t["in_reply_to_user_id"]
  t6 = t["in_reply_to_screen_name"]
  t7 = t["in_reply_to_status_id"]
  t8 = t["retweet_count"]
  t9 = t["contributors"]
  t10 = t["user"]["id_str"]
  try:
    t11 = t['geo']['coordinates'][0]
    t12 = t['geo']['coordinates'][1]
  except TypeError:
    t11 = None
    t12 = None

  inserts = (t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12)
  all_t.append(inserts)

for b in range(batchnumber):
  current = time.time()  
  try:
    cursor.executemany("INSERT INTO Twitter VALUES(?,?,?,?,?,?,?,?,?,?,?,?);",all_t[(b*6000):((b+1)*6000)])
  except sqlite3.IntegrityError:
    pass
  time2 = time.time()
  diff = time2-current
  acc += float(diff)
  all_acc.append(acc)

dtime120 = dtime120 + all_acc
print(dtime120)  

user_result = cursor.execute('SELECT count(*) from User')
userread = user_result.fetchone()

print(f"D Number of rows in user table = {userread}")


geo_result = cursor.execute('SELECT count(*) from Geo')
georread = geo_result.fetchone()

print(f"D Number of rows in geo table = {georread}")


twitter_result = cursor.execute('SELECT count(*) from Twitter')
twitteread = twitter_result.fetchone()

print(twitteread)
print(f"D Number of rows in twitter table = {twitteread}")



#e.  Plot the resulting runtimes (# of tweets versus runtimes) using matplotlib for 1-a, 1-b, 1-c, and 1-d. How does the runtime compare?

x_value = np.arange(1,120001)
x_values_d = np.linspace(1,120000,20)
plt.figure()
plt.plot(x_value,atime120,'r--',label='A')
plt.plot(x_value,btime120,'b--',label='B')
plt.plot(x_value,ctime120,'g--',label='C')
plt.plot(x_values_d,dtime120,'k--',label='D')
plt.legend()
plt.xlabel('Number of Tweets')
plt.ylabel('Time to Process (s)')
plt.title('Time to Process 120k Tweets')
plt.show()

#Part 2

#a.  Write and execute a SQL query to find the average longitude and latitude value for each user ID. This query does not need the User table because User ID is a foreign key in the Tweet table. 


time1 = time.time()
result = cursor.execute('SELECT user_id, AVG(Longitude), AVG(Latitude) FROM Twitter, Geo WHERE Twitter.Long = Geo.Longitude and Twitter.Lat = Geo.Latitude GROUP BY user_id;')
result2a = result.fetchone()

print(f"Results of part2a (one row fetched) = {result2a}")

time2 = time.time()

dpart2a1 = time2-time1
print(f"Part2a After 1 loops, {dpart2a1}s have passed")

#b.  Re-execute the SQL query in part 2-a 5 times and 20 times and measure the total runtime (just re-run the same exact query multiple times using a for-loop, it is as simple as it looks). Does the runtime scale linearly? 

time1 = time.time()

for i in range(5):
  result = cursor.execute('SELECT user_id, AVG(Longitude), AVG(Latitude) FROM Twitter, Geo WHERE Twitter.Long = Geo.Longitude and Twitter.Lat = Geo.Latitude GROUP BY user_id;')
  result2a = result.fetchone()

time2 = time.time()

dpart2b5 = time2-time1

print(f"Part2b After 5 loops, {dpart2b5}s have passed")

time1 = time.time()

for i in range(20):
  result = cursor.execute('SELECT user_id, AVG(Longitude), AVG(Latitude) FROM Twitter, Geo WHERE Twitter.Long = Geo.Longitude and Twitter.Lat = Geo.Latitude GROUP BY user_id;')
  result2a = result.fetchone()

time2 = time.time()

dpart2b20 = time2-time1

print(f"Part2b After 20 loops, {dpart2b20}s have passed")

bscale1 = dpart2b5/dpart2a1
bscale2 = dpart2b20/dpart2a1
if bscale1 == 5:
  print("Data scales linearly")
elif bscale1 != 5:
  print(f"Data does not scale linearly (5x) as scale is {bscale1}")
if bscale2 == 20:
  print("Data scales linearly")
elif bscale2 != 20:
  print(f"Data does not scale linearly (20x) as scale is {bscale2}")
#c.  Write the equivalent of the 2-a query in python (without using SQL) by reading it from the file with 600,000 tweets.

time1 = time.time()
file = open('oneday_tweets120k.txt','r', encoding="utf8")



user_valid = []

geo_long_valid = []
geo_lat_valid = []





for i in range(120000):
  
  tweet = file.readline()
  tdict = json.loads(tweet.encode('utf8'))
  
  user_valid.append(tdict["user"]["id_str"])
  

  try:
    geo_long_valid.append(tdict["geo"]["coordinates"][0])
  except TypeError:
    geo_long_valid.append(None)
  try:
    geo_lat_valid.append(tdict["geo"]["coordinates"][1])  
  except TypeError:
    geo_lat_valid.append(None) 

user_avglocation = {}

for u in range(len(user_valid)):

  try:
    lat_long = geo_long_valid[u] + geo_lat_valid[u]

    if user_valid[u] not in user_avglocation.keys():
      user_avglocation[user_valid[u]] = [lat_long]
    elif user_valid[u] in user_avglocation.keys():
      user_avglocation[user_valid[u]] += [lat_long]
  except TypeError:
    pass 
for key,values in user_avglocation.items():
  average = sum(values)/len(values)
  user_avglocation[key] = average

time2 = time.time()
djson1 = time2-time1
print(f"Using json, after one loop = {djson1}")

#d.  Re-execute the query in part 2-c 5 times and 20 times and measure the total runtime. Does the runtime scale linearly? 
time1 = time.time()
for i in range(5):

  file = open('oneday_tweets120k.txt','r', encoding="utf8")



  user_valid = []

  geo_long_valid = []
  geo_lat_valid = []





  for i in range(120000):
    
    tweet = file.readline()
    tdict = json.loads(tweet.encode('utf8'))
    
    user_valid.append(tdict["user"]["id_str"])
    

    try:
      geo_long_valid.append(tdict["geo"]["coordinates"][0])
    except TypeError:
      geo_long_valid.append(None)
    try:
      geo_lat_valid.append(tdict["geo"]["coordinates"][1])  
    except TypeError:
      geo_lat_valid.append(None) 

  user_avglocation = {}

  for u in range(len(user_valid)):


    try:
      lat_long = geo_long_valid[u] + geo_lat_valid[u]

      if user_valid[u] not in user_avglocation.keys():
        user_avglocation[user_valid[u]] = [lat_long]
      elif user_valid[u] in user_avglocation.keys():
        user_avglocation[user_valid[u]] += [lat_long]
    except TypeError:
      pass 
  for key,values in user_avglocation.items():
    average = sum(values)/len(values)
    user_avglocation[key] = average

time2 = time.time()
djson5 = time2-time1
print(f"Using json, after five loop = {djson5}")


time1 = time.time()
for i in range(20):

  file = open('oneday_tweets120k.txt','r', encoding="utf8")



  user_valid = []

  geo_long_valid = []
  geo_lat_valid = []





  for i in range(120000):
    
    tweet = file.readline()
    tdict = json.loads(tweet.encode('utf8'))
    
    user_valid.append(tdict["user"]["id_str"])
    

    try:
      geo_long_valid.append(tdict["geo"]["coordinates"][0])
    except TypeError:
      geo_long_valid.append(None)
    try:
      geo_lat_valid.append(tdict["geo"]["coordinates"][1])  
    except TypeError:
      geo_lat_valid.append(None) 

  user_avglocation = {}

  for u in range(len(user_valid)):
    try:
      lat_long = geo_long_valid[u] + geo_lat_valid[u]
 
      if user_valid[u] not in user_avglocation.keys():
        user_avglocation[user_valid[u]] = [lat_long]
      elif user_valid[u] in user_avglocation.keys():
        user_avglocation[user_valid[u]] += [lat_long]
    except TypeError:
      pass 
  for key,values in user_avglocation.items():
    average = sum(values)/len(values)
    user_avglocation[key] = average

time2 = time.time()
djson20 = time2-time1
print(f"Using json, after 20 loop = {djson20}")

dscale1 = djson5/djson1
dscale2 = djson20/djson1
if dscale1 == 5:
  print("Data scales linearly")
elif dscale1 != 5:
  print(f"Data does not scale linearly (5x) as scale is {dscale1}")
if dscale2 == 20:
  print("Data scales linearly")
elif dscale2 != 20:
  print(f"Data does not scale linearly (20x) as scale is {dscale2}")

#e.  Write the equivalent of the 2-a query in python by using regular expressions instead of json.loads(). Do not use json.loads() here. 
time1 = time.time()
file = open('oneday_tweets120k.txt','r', encoding="utf8")

all_userse = {}
all_loce = []



for i in range(120000):
  line = file.readline()
  line = str(line)
  try:
    regex1 = re.compile('"id_str":"([^"]*)"')
    user = regex1.findall(line)
    if user not in all_userse.keys():
      all_userse[user] = []



    all_userse.append(user)
    regex2 = re.compile('"geo":\{"type":"\w*","coordinates":\[([^,]*),')
    lo = regex2.findall(line)
    
    regex3 = re.compile('"geo":\{"type":"\w*","coordinates":\[[^,]*,([^,]*)\}')
    la = regex3.findall(line)
    loc = lo+la

    all_userse[user] += [loc]
  except TypeError:
    pass

for key,values in all_userse.items():
  average = sum(values)/len(values)
  all_userse[key] = average


time2 = time.time()
dregex1 = time2-time1

print(f"Using regular expressions, after one loop = {dregex1}")

#f.  Re-execute the query in part 2-e 5 times and 20 times and measure the total runtime. Does the runtime scale linearly? 

time1 = time.time()
file = open('oneday_tweets120k.txt','r', encoding="utf8")

all_userse = {}
all_loce = []


for j in range(5):
  for i in range(120000):
    line = file.readline()
    line = str(line)
    try:
      regex1 = re.compile('"id_str":"([^"]*)"')
      user = regex1.findall(line)
      if user not in all_userse.keys():
        all_userse[user] = []



      all_userse.append(user)
      regex2 = re.compile('"geo":\{"type":"\w*","coordinates":\[([^,]*),')
      lo = regex2.findall(line)
      
      regex3 = re.compile('"geo":\{"type":"\w*","coordinates":\[[^,]*,([^,]*)\}')
      la = regex3.findall(line)
      loc = lo+la

      all_userse[user] += [loc]
    except TypeError:
      pass

  for key,values in all_userse.items():
    average = sum(values)/len(values)
    all_userse[key] = average


time2 = time.time()
dregex5 = time2-time1
print(f"Using regular expressions, after five loops = {dregex5}")


#f

time1 = time.time()
file = open('oneday_tweets120k.txt','r', encoding="utf8")

all_userse = {}
all_loce = []


for j in range(20):
  for i in range(120000):
    line = file.readline()
    line = str(line)
    try:
      regex1 = re.compile('"id_str":"([^"]*)"')
      user = regex1.findall(line)
      if user not in all_userse.keys():
        all_userse[user] = []



      all_userse.append(user)
      regex2 = re.compile('"geo":\{"type":"\w*","coordinates":\[([^,]*),')
      lo = regex2.findall(line)
      
      regex3 = re.compile('"geo":\{"type":"\w*","coordinates":\[[^,]*,([^,]*)\}')
      la = regex3.findall(line)
      loc = lo+la

      all_userse[user] += [loc]
    except TypeError:
      pass

  for key,values in all_userse.items():
    average = sum(values)/len(values)
    all_userse[key] = average


time2 = time.time()
dregex20 = time2-time1
print(f"Using regular expressions, after 20 loops = {dregex20}")

fscale1 = dregex5/dregex1
fscale2 = dregex20/dregex1
if fscale1 == 5:
  print("Data scales linearly")
elif fscale1 != 5:
  print(f"Data does not scale linearly (5x) as scale is {fscale1}")
if fscale2 == 20:
  print("Data scales linearly")
elif fscale2 != 20:
  print(f"Data does not scale linearly (20x) as scale is {fscale2}")


#Part 3

#a.  Using the database with 600,000 tweets, create a new table that corresponds to the join of all 3 tables in your database, including records without a geo location. 
#This is the equivalent of a materialized view but since SQLite does not support MVs, we will use CREATE TABLE AS SELECT (instead of CREATE MATERIALIZED VIEW AS SELECT).

view = '''CREATE TABLE EveryTweet AS

Select * from (Twitter LEFT JOIN Geo) LEFT JOIN User
on Twitter.Long = Geo.Longitude and Twitter.Lat = Geo.Latitude and Twitter.user_id = User.id  ;
'''

drop4 = 'Drop Table All'
cursor.execute(drop4)

cursor.execute(view) 

#b.  Export the contents of your table from 3-a into a new JSON file (i.e., create your own JSON file with just the keys you extracted). 


import pandas as pd
import json

amount = cursor.execute('SELECT count(*) from EveryTweet')
count = amount.fetchone()
count = count[0]
rows = cursor.execute('SELECT * from EveryTweet')

dtweets = {}

columnnameslist = []
colnames = cursor.description
for col in colnames:

    dtweets[col[0]] = []
    columnnameslist.append(col[0]) 




  
row = rows.fetchall()
df = pd.DataFrame(row, columns=columnnameslist)

result = df.to_json(orient="columns")

parsed = json.loads(result)

jsonstr = json.dumps(parsed, indent=4)  

jsonfile = open("final120k.json", "w")
jsonfile.write(jsonstr)
jsonfile.close()

#cExport the contents of your table from 3-a into a .csv (comma separated value) file. 

amount = cursor.execute('SELECT count(*) from EveryTweet')
count = amount.fetchone()
count = count[0]
rows = cursor.execute('SELECT * from EveryTweet')

dtweets = {}

columnnameslist = []
colnames = cursor.description
for col in colnames:

    dtweets[col[0]] = []
    columnnameslist.append(col[0]) 




  
row = rows.fetchall()
df = pd.DataFrame(row, columns=columnnameslist)
csv_data = df.to_csv()
csvfile = open('csvfinal120k.csv', 'w',encoding='utf8')
csvfile.write(csv_data)
csvfile.close()
