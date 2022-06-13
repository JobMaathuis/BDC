#!/usr/bin/env python3

import sys
import csv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.stat import Correlation
from pyspark.sql.window import Window


def get_result(file):
        results = []
        spark = SparkSession.builder.master('local[16]').appName('sparkdf').getOrCreate()
        df = spark.read.csv(file, sep=r'\t', header=False, inferSchema=True)

        #Q1
        a1 = df.select('_c11').distinct()
        results.append([1, a1.count(), a1._jdf.queryExecution().simpleString()])

        #Q2
        a2 = df.groupby('_c0').count().agg({'count' : 'mean'})
        results.append([2, a2.first()[0], a2._jdf.queryExecution().simpleString()])

        #Q3
        a3 = df.where(df._c13 != '-').groupBy("_c13").count().sort("count", ascending=False)
        results.append([3, a3.first()[0], a3._jdf.queryExecution().simpleString()])

        #Q4
        a4 = df.groupby('_c11').agg({'_c2' : 'mean'})
        results.append([4, a4.first()[1], a4._jdf.queryExecution().simpleString()])

        #Q5
        a5 = df.where(df._c11 != '-').groupby('_c11').count().sort('count', ascending=False)
        results.append([5, [i[0] for i in a5.take(10)], a5._jdf.queryExecution().simpleString()])

        #Q6
        a6 = df.where(df._c11 != '-')\
                .withColumn('same_size', f.when((df['_c7'] - df['_c6']) > (df['_c2'] * 0.9), 1))\
                .filter(f.col('same_size').between(0, 2))\
                .groupBy('_c11')\
                .count()\
                .sort('count', ascending=False)
        results.append([6, [i[0] for i in a6.take(10)], a5._jdf.queryExecution().simpleString()])

        #Q7
        a7 = df.where(df._c12 != '-')\
                .withColumn('word', f.explode(f.split(f.col('_c12'), ' ')))\
                .groupBy('word')\
                .count()\
                .sort('count', ascending=False)
        results.append([7, [i[0] for i in a7.take(10)], a7._jdf.queryExecution().simpleString()])

        #Q8
        a8 = df.where(df._c12 != '-')\
                .withColumn('word', f.explode(f.split(f.col('_c12'), ' ')))\
                .groupBy('word')\
                .count()\
                .sort('count', ascending=True)
        results.append([8, [i[0] for i in a8.take(10)], a8._jdf.queryExecution().simpleString()])

        #Q9
        a9 = df.where(df._c11 != '-')\
                .withColumn('same_size', f.when((df['_c7'] - df['_c6']) > (df['_c2'] * 0.9), 1))\
                .filter(f.col('same_size').between(0, 2))\
                .withColumn('word', f.explode(f.split(f.col('_c12'), ' ')))\
                .groupBy('word')\
                .count()\
                .sort('count', ascending=False)
        results.append([9, [i[0] for i in a9.take(10)],  a9._jdf.queryExecution().simpleString()])

        #Q10
        a10 = df.select('_c0', '_c2')\
                .withColumn('counts', f.count('_c0').over(Window.partitionBy('_c2')))\
                .dropDuplicates(['_c0'])
        results.append([10, a10.stat.corr('_c2', 'counts'), a10._jdf.queryExecution().simpleString()])

        spark.stop()

        return results


def write_output(results, output=sys.stdout):
        csv_writer = csv.writer(output, dialect='excel')
        for row in results:
                csv_writer.writerow(row)

if __name__ == '__main__':
        results = get_result(sys.argv[1])
        write_output(results)

    