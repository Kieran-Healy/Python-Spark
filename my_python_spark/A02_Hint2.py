# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import pyspark

import calendar
import datetime

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    if (len(params) == 7):
        res = tuple(params)

    # 5. We return res
    return res

def get_day_of_week(date):
    # 1. We create the output variable
    res = calendar.day_name[(datetime.datetime.strptime(date, "%d-%m-%Y")).weekday()]

    # 2. We return res
    return res

def getRDD(my_tuple):
    myList = my_tuple[4].split(' ')
    date = get_day_of_week(myList[0])
    time = myList[1].split(':')
    hour = time[0]
    key = date + "_" + hour
    res = (key, 1)
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name):
    inputRDD = sc.textFile(my_dataset_dir)

    tuplesRDD = inputRDD.map(process_line)

    filterRDD = tuplesRDD.filter(lambda my_tuple: (int(my_tuple[0]) == 0) and (int(my_tuple[5]) == 0) and (my_tuple[1] == station_name))

    filterRDD.persist()

    total = filterRDD.count()
    newRDD = filterRDD.map(getRDD)

    testRDD = newRDD.reduceByKey(lambda x, y: x + y)
    percentageRDD = testRDD.map(lambda x: (x[0], (x[1], ((x[1] * 1.0) / (total * 1.0)) * 100.0)))

    data = percentageRDD.collect()
    for y in data:
        print(y)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input parameters as needed
    station_name = "Fitzgerald's Park"

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../my_dataset/"
    my_databricks_path = "/FileStore/tables/A02/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path
    else:
        my_dataset_dir = my_databricks_path

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, station_name)
