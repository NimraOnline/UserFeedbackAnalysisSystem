#!/usr/bin/env python3

from updateUsers import *
from updateFeedback import *
import pandas
from datetime import datetime



#Prepared Statement to update new users into the database
fetch_new_users_from_source(updateDB=True)
#Gets the last date of the feedback submitted in out database
last_date_of_retrieval = get_last_timestamp()
todays_date = datetime.now()



time_difference = todays_date.date() - last_date_of_retrieval[0].date()

has_been_week = True if time_difference.days >= 7 else False

print(time_difference.days, 'Days since last feedback data was loaded')
if has_been_week:
	print('Loading new data to database...')
	fetch_new_feedback_from_source(updateDB=True, time=last_date_of_retrieval)
else:
	print('New data will be loaded in ', 7-time_difference.days, ' days')


