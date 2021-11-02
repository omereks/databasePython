How to use:

1. make sure ypu have python in your windows
2. download initialDB.py into a folder
3. download https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_csv_2021-04-28.zip 
4. unzip into the same folder with initialDB
5. open terminal in the folder
6. run-  python -m pip install mysql-connector-python
7. run - python -m pip install pandas
8. install mysql server 8.* on port 3306
9. use username: root
10. password: root
11. install mysql workbench
12. open mysql workbench and add mysql connection on local

in the python code we have 2 flags

FLAG100 = 0 -> add all table (may take 40 mins)
FLAG100 = 1 -> add only 100 first rows

FLAGDEl = 0 -> for first run
FLAGDEl = 1 -> delete the exisitin tables (for second run)

14. run on terminal- python initialDB.py
15. wait 40 mins
16. check if our schemas exist in mysql workbench
