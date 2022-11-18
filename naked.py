#Imports  multiple libraries
import requests
import json
import datetime
import time
import yaml
import logging
import logging.config
import yaml
import mysql.connector

#Prints a text that signifies that this file was executed 
from configparser import ConfigParser
from datetime import datetime
from mysql.connector import Error
print('Asteroid processing service')

# Initiating and reading config values
print('Loading configuration from file')

# Loading logging configuration
with open('./log_worker.yaml', 'r') as stream:
    log_config = yaml.safe_load(stream)

logging.config.dictConfig(log_config)

# Creating logger
logger = logging.getLogger('root')

#Gets the api key and url
try:
	config = ConfigParser()
	config.read('config.ini')

	nasa_api_key = config.get('nasa', 'api_key')
	nasa_api_url = config.get('nasa', 'api_url')

	mysql_config_mysql_host = config.get('mysql_config', 'mysql_host')
	mysql_config_mysql_db = config.get('mysql_config', 'mysql_db')
	mysql_config_mysql_user = config.get('mysql_config', 'mysql_user')
	mysql_config_mysql_pass = config.get('mysql_config', 'mysql_pass')
except:
	logger.exception('')
logger.info('DONE')

def init_db():
	global connection
	connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)

def get_cursor():
	global connection
	try:
		connection.ping(reconnect=True, attempts=1, delay=0)
		connection.commit()
	except mysql.connector.Error as err:
		logger.error("No connection to db " + str(err))
		connection = init_db()
		connection.commit()
	return connection.cursor()

# Check if asteroid exists in db
def mysql_check_if_ast_exists_in_db(request_day, ast_id):
	records = []
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute("SELECT count(*) FROM ast_daily WHERE `create_date` = '" + str(request_day) + "' AND `ast_id` = '" + str(ast_id) + "'")
		records = cursor.fetchall()
		connection.commit()
	except Error as e :
		logger.error("SELECT count(*) FROM ast_daily WHERE `create_date` = '" + str(request_day) + "' AND `ast_id` = '" + str(ast_id) + "'")
		logger.error('Problem checking if asteroid exists: ' + str(e))
		pass
	return records[0][0]

# Asteroid value insert
def mysql_insert_ast_into_db(create_date, hazardous, name, url, diam_min, diam_max, ts, dt_utc, dt_local, speed, distance, ast_id):
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute( "INSERT INTO `ast_daily` (`create_date`, `hazardous`, `name`, `url`, `diam_min`, `diam_max`, `ts`, `dt_utc`, `dt_local`, `speed`, `distance`, `ast_id`) VALUES ('" + str(create_date) + "', '" + str(hazardous) + "', '" + str(name) + "', '" + str(url) + "', '" + str(diam_min) + "', '" + str(diam_max) + "', '" + str(ts) + "', '" + str(dt_utc) + "', '" + str(dt_local) + "', '" + str(speed) + "', '" + str(distance) + "', '" + str(ast_id) + "')")
		connection.commit()
	except Error as e :
		logger.error( "INSERT INTO `ast_daily` (`create_date`, `hazardous`, `name`, `url`, `diam_min`, `diam_max`, `ts`, `dt_utc`, `dt_local`, `speed`, `distance`, `ast_id`) VALUES ('" + str(create_date) + "', '" + str(hazardous) + "', '" + str(name) + "', '" + str(url) + "', '" + str(diam_min) + "', '" + str(diam_max) + "', '" + str(ts) + "', '" + str(dt_utc) + "', '" + str(dt_local) + "', '" + str(speed) + "', '" + str(distance) + "', '" + str(ast_id) + "')")
		logger.error('Problem inserting asteroid values into DB: ' + str(e))
		pass

def push_asteroids_arrays_to_db(request_day, ast_array, hazardous):
	for asteroid in ast_array:
		if mysql_check_if_ast_exists_in_db(request_day, asteroid[9]) == 0:
			logger.debug("Asteroid NOT in db")
			mysql_insert_ast_into_db(request_day, hazardous, asteroid[0], asteroid[1], asteroid[2], asteroid[3], asteroid[4], asteroid[5], asteroid[6], asteroid[7], asteroid[8], asteroid[9])
		else:
			logger.debug("Asteroid already IN DB")

def getting_ast_count():
	dt = datetime.now()
	request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
	r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)
	if r.status_code == 200:
		json_data = json.loads(r.text)
		return int(json_data['element_count'])
count_total = getting_ast_count()

def pos_ast_pass_dist():
	dist_list = []
	dt = datetime.now()
	request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
	r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)
	if r.status_code == 200:
		json_data = json.loads(r.text)
		for val in json_data['near_earth_objects'][request_date]:
			if 'close_approach_data' in val:
				if len(val['close_approach_data']) > 0:
					if 'miss_distance' in val['close_approach_data'][0]:
						if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
							tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
							dist_list.append(tmp_ast_miss_dist)
	#dist_list.append(0)
	return dist_list
	
dist_list = pos_ast_pass_dist()

if __name__ == "__main__":

	connection = None
	connection = False

	init_db()

	# Opening connection to mysql DB
	logger.info('Connecting to MySQL DB')
	try:
		# connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)
		cursor = get_cursor()
		if connection.is_connected():
			db_Info = connection.get_server_info()
			logger.info('Connected to MySQL database. MySQL Server version on ' + str(db_Info))
			cursor = connection.cursor()
			cursor.execute("select database();")
			record = cursor.fetchone()
			logger.debug('Your connected to - ' + str(record))
			connection.commit()
	except Error as e :
		logger.error('Error while connecting to MySQL' + str(e))

	# Getting todays date
	dt = datetime.now()
	request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
	print("Generated today's date: " + str(request_date))

	#Prints that you're requesting info from NASA and r is the request
	print("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key))
	r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)

	#Prints the requests response from NASA
	print("Response status code: " + str(r.status_code))
	print("Response headers: " + str(r.headers))
	print("Response content: " + str(r.text))

	#If the status code is 200, then the code continues in
	if r.status_code == 200:

		#json_data is the requests data
		json_data = json.loads(r.text)

		#defines 2 types of asteroid arrays
		ast_safe = []
		ast_hazardous = []

		#If there's an "element_count" in json data then the code continues in
		if 'element_count' in json_data:
			#gives ast_count the numeric value of how many asteroids there are and prints out a text saying it
			ast_count = int(json_data['element_count'])
			print("Asteroid count today: " + str(ast_count))

			#If there are any asteroids the code continues in
			if ast_count > 0:
				#Creates a for loop with all asteroids that are "near earth"
				for val in json_data['near_earth_objects'][request_date]:
					#Checks whether there are all these values for each asteroid, if there are the code continues in
					if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
						#Gives asteroid a name and an url(?) for this loop
						tmp_ast_name = val['name']
						tmp_ast_nasa_jpl_url = val['nasa_jpl_url']
						#Getting id of asteroid
						tmp_ast_id = val['id']
						#If in value of "estimated diameter" there are kilometers the code continues in
						if 'kilometers' in val['estimated_diameter']:
							#If there are values for estimated diameter min and max the code continues in and keeps those values for this loop
							if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']:
								tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)
								tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3)
							#If there isn't estimated diameter min and max it gives a value of -2(?)
							else:
								tmp_ast_diam_min = -2
								tmp_ast_diam_max = -2
						#If there  isn't a "kilometers" in "estimated diameter" then it sets the min and max of diameter as -1(?)
						else:
							tmp_ast_diam_min = -1
							tmp_ast_diam_max = -1

						#Gives a value whether the asteroid is hazardous or not
						tmp_ast_hazardous = val['is_potentially_hazardous_asteroid']

						#If there's data of close approach the code continues in
						if len(val['close_approach_data']) > 0:
							#If there are all these values in "close approach data" then the code continues in
							if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]:
								#Sets the values of all these for times of close approach
								tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000)
								tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
								tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')

								#If there are "kilometers per hour" defined in "close approach data" then the code continues in
								if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']:
									#Assigns the asteroid it's speed for this loop
									tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']))
								#if the "kph" isn't defined in "close approach data" then the value of speed is -1
								else:
									tmp_ast_speed = -1

								#If there are "kilometers" defined in "close approach data"'s "miss distance" then the code continues in
								if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
									#Assigns the asteroid it's miss distance
									tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
								#If there weren't "kilometers" for the "miss distance" it's value is assigned as -1
								else:
									tmp_ast_miss_dist = -1
							#If there weren't all those values in "close approach data" then these would be the default values
							else:
								tmp_ast_close_appr_ts = -1
								tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59"
								tmp_ast_close_appr_dt = "1969-12-31 23:59:59"
						#If there's no "close approach data" then this would print out and those would be the default values
						else:
							print("No close approach data in message")
							tmp_ast_close_appr_ts = 0
							tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
							tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
							tmp_ast_speed = -1
							tmp_ast_miss_dist = -1

						#Prints out a lot of information about the asteroid
						print("------------------------------------------------------- >>")
						print("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
						print("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
						print("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km")

						# Adding asteroid data to the corresponding array
						if tmp_ast_hazardous == True:
							ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist, tmp_ast_id])
						else:
							ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist, tmp_ast_id])
			#If asteroid count was 0 then this message would print
			else:
				print("No asteroids are going to hit earth today")

		#Prints out both hazardous and safe asteroids
		print("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))

		#If there's atleast 1 hazardous asteroid the code continues in
		if len(ast_hazardous) > 0:

			#Sorts the asteroids
			ast_hazardous.sort(key = lambda x: x[4], reverse=False)

			#Prints out about the hazardous asteroids
			print("Today's possible apocalypse (asteroid impact on earth) times:")
			#creates a loop to print info abouch each hazardous asteroid
			for asteroid in ast_hazardous:
				print(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))
			#Sorts the asteroids again
			ast_hazardous.sort(key = lambda x: x[8], reverse=False)
			#Prints info about the closest passing asteroid
			print("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))
			push_asteroids_arrays_to_db(request_date, ast_hazardous, 1)
		#If there are no hazardous asteroids this message is printed
		else:
			print("No asteroids close passing earth today")

		if len(ast_safe) > 0:
			push_asteroids_arrays_to_db(request_date, ast_safe, 0)
	#This happens when the response code was not 200
	else:
		print("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
