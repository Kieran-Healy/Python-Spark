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

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def is_valid_entry(my_tuple):
    res = False

    if (int(my_tuple[0]) == 0) and (int(my_tuple[5]) == 0):
        res = True

    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir):
    inputRDD = sc.textFile(my_dataset_dir)

    tuplesRDD = inputRDD.map(process_line)

    filterRDD = tuplesRDD.filter(is_valid_entry)

    #filterRDD = tuplesRDD.filter( lambda my_tuple: (int(my_tuple[0]) == 0) and (int(my_tuple[5]) == 0) )

    infoRDD = filterRDD.map(lambda x : (x[1], 1))
    solutionRDD = infoRDD.reduceByKey(lambda x, y: x + y)
    sortedList = solutionRDD.takeOrdered(30, key = lambda x: -x[1])

    for item in sortedList:
        print(item)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Local or Databricks
    local_False_databricks_True = False

    # 2. We set the path to my_dataset and my_result
    my_local_path = "../my_dataset/"
    my_databricks_path = "/FileStore/tables/A02/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path
    else:
        my_dataset_dir = my_databricks_path

    # 3. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 4. We call to our main function
    my_main(sc, my_dataset_dir)
