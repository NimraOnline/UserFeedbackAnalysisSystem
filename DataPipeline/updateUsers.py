#!/usr/bin/env python3

import mysql.connector
import pymysql
from sqlalchemy import create_engine
import pandas as pd
from EduTech.EduTech import *

Data = Data()
DB = DB()

#Ideally this would be an API that was provided by the developers but for project purposes we are manually checking "new users"
def get_newusers_API():
	#From Data Vendor's Database (Using same as our DB for project purposes)
	users = pd.read_csv(Data.path)
	connection = mysql.connector.connect(
		host=DB.host,
		user=DB.user,
		password=DB.password,
		database=DB.database
	)
	cursor = connection.cursor()
	column_name = "Username"
	query = f"SELECT {column_name} FROM Users"
	cursor.execute(query)
	currentUsers = cursor.fetchall()
	cursor.close()
	connection.close()

	#all users (new included)
	allUsers = users['UserID']

	currentUsers = {username for (username,) in currentUsers}
	new_users = set(allUsers) - set(currentUsers)

	return(new_users, users)

def fetch_new_users_from_source(updateDB=False):
	#Make API call
	response = get_newusers_API()

	if len(response[0]) > 0:
		if updateDB:
			df = response[1].where(response[1]['UserID'].isin(response[0]))
			engine = create_engine("mysql+pymysql://" + DB.user + ":" + DB.password + "@" + DB.host + "/" + DB.database)
			insert_into_db(engine=engine, data=df)
			engine.dispose()
			print("Database updated")
		else:
			return (response[0])
	else:
		print("No new users!")
		
	return (response[0])

def insert_into_db(engine, data):
	user_featuers = list(data.columns)
	#Incase the data vendor changes the column names
	column_mapping = {
		user_featuers[0]: 'Username',
	    user_featuers[1]: 'FirstName',
	    user_featuers[2]: 'LastName',
	    user_featuers[3]: 'Email',
	}
	data.rename(columns=column_mapping, inplace=True)
	data.to_sql(name='Users', con=engine, if_exists='append', index=False)

	




