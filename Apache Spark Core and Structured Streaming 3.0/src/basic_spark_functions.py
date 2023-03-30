from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)



# Map. Filter, Reduce
# These functions work on a list of data!!
# map syntax: map(function, sequence) || map(lambda x: x**2), arr)
# filter syntax: filter(lambda, function) || filter(lambda x:x%2!=0, l) , used to filter records based on true values given by first function for each element of the list
# reduce syntax: reduce(function, list) ||


# Filter--->
my_list = [11,12,17,14,10,13]
odd_numbers = list(filter(lambda x: x%2 !=0, my_list))
print("Odd Numbers=> {}".format(odd_numbers))


# Map--->
def square(x):
    return x**2

squared_numbers = list(map(square, my_list))
print("Squared numbers=> {}".format(squared_numbers))

# Reduce--->
from functools import reduce
max_number = reduce(lambda x,y : x if x>y else y, my_list)
print("max number is=> {}".format(max_number))


# ----------------------------------------------------------------------------------------
# The above functions use python's single thread execution ie, no parallelism is involved
#  Now we implement the same using spark's functions
# ----------------------------------------------------------------------------------------


# There are 2 ways to generate a spark RDD
# 1) Using Parallelise
# 2) Using TextFile

rdd = sc.parallelize(my_list)
print("Number of partitions=> {}".format(rdd.getNumPartitions()))

odd_numbers_rdd = rdd.filter(lambda x: x%2!=0)
print("odd_numbers_rdd=> ",odd_numbers_rdd.collect())


squared_numbers_rdd = rdd.map(square)
print("squared_numbers_rdd=> ",squared_numbers_rdd.collect())

sorted_numbers_rdd = rdd.sortBy(lambda x:x)
print("sorted_numbers_rdd=> ",sorted_numbers_rdd.collect())

max_number = rdd.reduce(lambda x,y: x if x>y else x)
# Note: here reduce gives a single value as output no rdd was return hence if we call collect operation then it will not work
print("max_number=> ",max_number)
