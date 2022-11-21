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
        assert connection.is_connected() == True
        print("Can connect to Database")
else:
    print("Connection to database is not working")
    quit()


print("------------------------")
print ("Testing to see if there are any false passing distances (dist less than given)")
distance = 5000
assert pos_low_pass_dist(distance) != distance
print("------------------------")
print("Everything is OK")