#!/usr/bin/env python3

import mysql.connector
import pymysql
from sqlalchemy import create_engine
import pandas as pd
from EduTech.EduTech import *
import datetime

Data = Data()
DB = DB()

#Ideally this would be an API that was provided by the developers but for project purposes we are manually checking "new feedback"
def get_newfeedback_API(time):
	#From Data Vendor's Database (Using same our DB for project purposes)
	connection = mysql.connector.connect(
		host=DB.host,
		user=DB.user,
		password=DB.password,
		database=DB.database
	)
	cursor = connection.cursor()
	value = time[0]
	query = f"SELECT * FROM FeedbackSubmissions WHERE Timestamp > (%s)"
	cursor.execute(query,(value,))
	newFeedback = cursor.fetchall()
	cursor.close()
	connection.close()

	return(newFeedback)



def get_last_timestamp():
	#From our MySQL database
	connection = mysql.connector.connect(
		host=DB.host,
		user=DB.user,
		password=DB.password,
		database=DB.database
	)
	cursor = connection.cursor()

	query = f"SELECT MAX(Timestamp) FROM FeedbackSubmissions;"
	cursor.execute(query)
	result = cursor.fetchone()
	cursor.close()
	connection.close()

	return result

def fetch_new_feedback_from_source(updateDB=False, time=0):
	response = get_newfeedback_API(time)

	if len(response) > 0:
		engine = create_engine("mysql+pymysql://" + DB.user + ":" + DB.password + "@" + DB.host + "/" + DB.database)
		feedbackSubmissions.to_sql(name='FeedbackSubmissions', con=engine, if_exists='append', index=False)
		print("uploaded!")
		engine.dispose()
	else:
		print("There are no new feedback submissions.")




