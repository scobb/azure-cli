# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------


"""
Utilities to save and load schema information for Spark DataFrames.

"""


from __future__ import print_function
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

schema2 = StructType(
    [StructField("fields",
                 ArrayType(
                     StructType(
                         [
                             StructField("metadata", StructType(), True),
                             StructField("name", StringType(), True),
                             StructField("nullable", BooleanType(), True),
                             StructField("type", StringType(), True)
                         ]
                     )
                     , True)
                 , True),
     StructField("type", StringType(), True)
     ])


def saveSchema(df, filename):
    schemaString = [df.schema.json()]
    schemaRDD = spark.sparkContext.parallelize(schemaString)
    schemaDF = spark.read.json(schemaRDD, schema2)
    schemaDF.coalesce(1).write.format("json").mode("overwrite").save(filename)
    print("Schema saved to", filename)
    sampleDF = spark.createDataFrame(df.take(1), df.schema)
    sample_filename = filename + '.sample'
    sampleDF.coalesce(1).write.format('json').mode('overwrite').save(sample_filename)
    print('Sample saved to', sample_filename)


def loadSchema(filename):
    sDF = spark.read.json(filename, schema2)
    return StructType.fromJson(sDF.first().asDict(True))
