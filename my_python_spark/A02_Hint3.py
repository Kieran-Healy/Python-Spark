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

def getRDD(my_tuple):
    x,y = my_tuple.split(" ")
    time = y.split(":")
    hour = int(time[0])
    minute = int(time[1])
    return (x,y,hour,minute)

current = ""
continued = []

def getTimes(x,measurement_time):
    global continued, current
    minute = x[3]
    hour = x[2]
    if len(continued) != 2:
        current = x[1]
        continued = [hour,minute]
        return ((x[0],x[1]),1)
    added = False
    minute2 = minute - measurement_time
    if minute2 >= 0:
        if continued[0] == hour and continued[1] == minute2:
            added = True
    else:
        if continued[0] == hour -1 and continued[1] == minute2 + 60:
            added = True
    continued = [hour,minute]
    if not added:
        current = x[1]
        return ((x[0],x[1]),1)
    else:
        return ((x[0],current),1)
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name, measurement_time):
    inputRDD = sc.textFile(my_dataset_dir)

    tuplesRDD = inputRDD.map(process_line)

    filterRDD = tuplesRDD.filter(lambda my_tuple: (int(my_tuple[0]) == 0) and (int(my_tuple[5]) == 0) and (my_tuple[1] == station_name))

    timeRDD = filterRDD.map(lambda x : getRDD(x[4]))

    newRDD = timeRDD.map(lambda x : getTimes(x,measurement_time))

    solutionRDD = newRDD.groupByKey().map(lambda x: (x[0],len(x[1])))

    x = solutionRDD.collect()
    for data in x:
        print(data)

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
    measurement_time = 5

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
    my_main(sc, my_dataset_dir, station_name, measurement_time)
