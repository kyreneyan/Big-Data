from pyspark import SparkContext
import csv
import sys

if __name__=='__main__':

    def mapper(partitionId, records):
        if partitionId == 0:
            next(records)
        reader = csv.reader(records)
        for r in reader:
            yield ((r[1].lower(), int(r[0][:4]), r[7].lower()), 1)
            

    if len(sys.argv) > 1：
        input_file = sys.argv[1]
        output_folder = sys.argv[2]
    else：
        '.'
 
    sc = SparkContext()

    data = sc.textFile(input_file).cache()

    data.mapPartitions(lambda x: csv.reader(x, delimiter=',', quotechar='"'))\
        .filter(lambda x: len(x)==18 and len(x[0])==10)\
        .map(lambda r: ((r[1].lower(), r[0][:4], r[7]), 1))\
        .reduceByKey(lambda x, y: x+y)\
        .map(lambda x: ((x[0][0], x[0][1]), (x[1], 1, x[1])))\
        .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], max(x[2], y[2])))\
        .mapValues(lambda x: (x[0], x[1], round(x[2]*100/x[0])))\
        .sortByKey()\
        .map(lambda x: x[0]+x[1])\
        .saveAsTextFile(output_folder)


