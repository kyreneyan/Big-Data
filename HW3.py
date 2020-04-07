from pyspark import SparkContext
import csv
import sys

if __name__=='__main__':
    
    input_file = sys.argv[1]
    output_folder = sys.argv[2]
 
    sc = SparkContext()

    data = sc.textFile(input_file)
    header = data.take(3)
    data = data.filter(lambda x: x not in header) \
               .mapPartitions(lambda data: csv.reader(data, delimiter=',', quotechar='"')) \
               .map(lambda data: ((data[1].lower(), data[0][:4], data[7]),1)) \
               .reduceByKey(lambda x, y: x+y) \
               .map(lambda x: ((x[0][0], x[0][1]), (x[1], 1, x[1]))) \
               .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], max(x[2], y[2]))) \
               .mapValues(lambda x: (x[0], x[1], round(x[2]*100/x[0]))) \
               .sortByKey() \
               .map(lambda x: x[0]+x[1]) \
               .map(to_csv) \
               .saveAsTextFile(output_folder)
