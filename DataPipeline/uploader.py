#!/usr/bin/env python3

from sqlalchemy import create_engine
import pandas
from EduTech.EduTech import *

allFeedback = EduTech.AllFeedback()

#Collecting data from sources
feedbackDF = pd.read_csv(allFeedback)

#Creating df based on the database schema and dependencies
feedbackSubmissions = feedbackDF[['SubmissionID', 'Sentiment', 'Comment', 'Timestamp', 'Location', 'Email', 'Categories']]

#Connecting to out DB to gather info like UserID and CategoryID to correspond with new Data
connection = mysql.connector.connect(
	host=DB.host,
	user=DB.user,
	password=DB.password,
	database=DB.database
)
cursor = connection.cursor()

#UserID from Users table, match on email
query = f"SELECT UserID, Email FROM Users"
cursor.execute(query)
userID_and_email = cursor.fetchall()
email_to_id = {email: i for i, email in userID_and_email}

#CategoryID from CategoryNames tables, match on Categories
query = f"SELECT CategoryID, CategoryName FROM FeedbackCategories"
cursor.execute(query)
catID_and_name = cursor.fetchall()
cat_to_id = {i : cat for cat, i in catID_and_name}

cursor.close()
connection.close

feedbackSubmissions['UserID'] = feedbackSubmissions['Email'].map(email_to_id)
feedbackSubmissions['CategoryID'] = feedbackSubmissions['Categories'].map(cat_to_id)

feedbackSubmissions = feedbackSubmissions.drop(['Email', 'Categories'], axis=1)
#repositioning (optional)
lasttwo = feedbackSubmissions.columns[-2:]
data = feedbackSubmissions[lasttwo]
feedbackSubmissions =feedbackSubmissions.drop(columns=lasttwo)

feedbackSubmissions.insert(1, lasttwo[0], data[lasttwo[0]])
feedbackSubmissions.insert(2, lasttwo[1], data[lasttwo[1]])


engine = create_engine("mysql+pymysql://" + DB.user + ":" + DB.password + "@" + DB.host + "/" + DB.database)


feedbackSubmissions = feedbackSubmissions.drop(['SubmissionID'], axis=1)
feedbackSubmissions.to_sql(name='FeedbackSubmissions', con=engine, if_exists='append', index=False)
print("uploaded!")
engine.dispose()
