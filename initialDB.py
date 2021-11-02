# pip install mysql-connector-python
# pip install pandas
import math

import mysql.connector
import pandas as pd

FLAG100 = 0
FLAGDEl = 1


def init_mysql():
    db = mysql.connector.connect(host="localhost",
                                 port="3307",
                                 user="root",
                                 passwd="root",
                                 )
    return db


db = init_mysql()
mycursor = db.cursor()

#drop existing schema:
if FLAGDEl == 1:
    print("delete all DB")
    mycursor.execute("DROP DATABASE `nurti`")

#create schema
print("create new DB")
mycursor.execute("CREATE SCHEMA `nurti`")

#creat tables:

#Food
print("create food table..")
mycursor.execute("CREATE TABLE `nurti`.`food` ( \
                    `food_id` INT NOT NULL, \
                    `description` VARCHAR(999) NULL, \
                    PRIMARY KEY (`food_id`))")


empdata = pd.read_csv('food.csv', index_col=False, delimiter = ',')
empdata.head()
print("start upload food..")
maxRows = 1048576
p = 0
for i,row in empdata.iterrows():
    break #todo
    food_id = row[0]
    food_description = row[2]
    query = "INSERT INTO `nurti`.`food` (`food_id`, `description`) VALUES (%s, %s);"
    if type(food_description) is float and math.isnan(food_description):
        food_description = "empty"
    val = (food_id, food_description)
    mycursor.execute(query, val)
    if i % 100000 == 0:
        print(str(p) + "%")
        p = p + 10
    if FLAG100 == 1 and i == 100:
        break
db.commit()

print("finished upload food")

#Nutrient
print("create nutrient table")
mycursor.execute("CREATE TABLE `nurti`.`nutrient` (" \
                "`nutrient_id` INT NOT NULL," \
                "`nutrient_name` VARCHAR(999) NULL," \
                "`unit` VARCHAR(45) NULL," \
                "PRIMARY KEY (`nutrient_id`));")


empdata = pd.read_csv('nutrient.csv', index_col=False, delimiter = ',')
empdata.head()
print("start upload nutrient..")
p = 0
for i,row in empdata.iterrows():
    break #TODO
    nutrient_id = row[0]
    nutrient_name = row[1]
    unit = row[2]
    query = "INSERT INTO `nurti`.`nutrient` (`nutrient_id`, `nutrient_name`, `unit`) VALUES (%s,%s,%s)"
    val = (nutrient_id, nutrient_name, unit)
    mycursor.execute(query, val)
    if FLAG100 == 1 and i == 100:
        break
    if i % 40 == 0:
        print(str(p) + "%")
        p = p + 10
db.commit()
print("finish upload nutrient")


#food_values
print("create food_values table")
mycursor.execute("CREATE TABLE `nurti`.`food_values` (" \
                    "`ID` INT NOT NULL," \
                    "`food_id` INT NULL," \
                    "`nutrient_id` INT NULL," \
                    "`amount` VARCHAR(45) NULL," \
                    "PRIMARY KEY (`ID`));")


empdata = pd.read_csv('food_nutrient.csv', index_col=False, delimiter = ',')
empdata.head()
print("start upload food_values..")
p = 0
for i,row in empdata.iterrows():
    ID = i+1
    food_id = row[1]
    nutrient_id = row[2]
    amount = row[3]
    query = "INSERT INTO `nurti`.`food_values` (`ID`, `food_id`, `nutrient_id`, `amount`) VALUES (%s,%s,%s,%s);"
    val = (ID, food_id, nutrient_id, amount)
    mycursor.execute(query, val)
    if FLAG100 == 1 and i == 100:
        break
    if i % 500000 == 0:
        print(str(p) + "%")
        p = p + 10
db.commit()
print("finish upload food_values")

#food_eaten
print("create food_eaten table")
mycursor.execute("CREATE TABLE `nurti`.`food_eaten` (" \
                    "`ID` INT NOT NULL," \
                    "`food_id` VARCHAR(45) NULL," \
                    "`amount` INT NULL," \
                    "`gender` TINYINT NULL," \
                    "PRIMARY KEY (`ID`));")

print("finish upload food_eaten")