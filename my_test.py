from naked import *
import mysql.connector

print("------------My test------------")
print("Testing whether the amount of asteroids in DB is more than 0")
if (count_total != None):
    if (count_total > 0): print("Connection to DB is working and asteroid count today = " + str(count_total))
    else:
        print("Asteroid count 0 or connection to db not working")
        print("Checking whether we can connect to database")
        connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)
        if (connection.is_connected() == True):
            print("Can connect to Database")
        else:
            print("Connection to database not working")
            quit()
else:
    print("Connection to database is not working")
    quit()

print("------------------------")
print ("Testing to see if there are any false passing distances (dist less than 5000)")

for val in dist_list:
    if (val < 50000000):
        print("There's a false miss distance in the distance list")
        quit()
print("There are no false distances")
print("------------------------")
print("Everything is OK")