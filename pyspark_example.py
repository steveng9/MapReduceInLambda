import time
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

text = "Moby-Dick.txt"
# text = "a.txt"
# text = "/Users/golobs/Documents/Reading/David-Foster-Wallace-Infinite-Jest-v2.0.txt"

num_executors = 1

# Create a SparkConf object and set the master URL to local[*]
conf = SparkConf() \
    .setAppName("WordCount") \
    .setMaster(f"local[{num_executors}]") \
    .set("spark.executor.instances", str(num_executors)) \
    .set("spark.executor.cores", "1") \
    .set("spark.executor.memory", "2g")

# Initialize SparkContext with this configuration
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()


def pretty_print_plan(rdd):
    for x in rdd.toDebugString().decode().split('\n'):
        print(x)

t1 = time.process_time() * 1000
text_file = sc.textFile(text)


t2 = time.process_time() * 1000
words = text_file.flatMap(lambda line: line.split(" "))
not_empty = words.filter(lambda x: x!='') 
key_values = not_empty.map(lambda word: (word, 1)) 
counts = key_values.reduceByKey(lambda a, b: a + b)


t3 = time.process_time() * 1000
Count=counts.count()  # Count = the number of different words

t4 = time.process_time() * 1000
Sum=counts.map(lambda x:x[1]).reduce(lambda x,y:x+y) # 


t5 = time.process_time() * 1000
print()
print('Different words=%5.0f, total words=%6.0f, mean no. occurances per word=%4.2f'%(Count,Sum,float(Sum)/Count))
print(round(t2 - t1, 2), round(t3 - t2, 2), round(t4 - t3, 2), round(t5 - t4, 2))