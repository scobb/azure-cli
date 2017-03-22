from pyspark.sql.types import DoubleType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Boston House Price - Scoring").getOrCreate()
sc = spark.sparkContext
trainedModel = LogisticRegressionModel
schema = ['sepal-length', 'sepal-width', 'petal-length', 'petal-width']

def init():
    global trainedModel
    trainedModel=LogisticRegressionModel.load('wasb://models@ritbhatrrs.blob.core.windows.net/irismodel1')

def run(inputString):
    import json
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql import SQLContext

    sqlcontext = SQLContext.getOrCreate(sc)
    input = json.loads(inputString)  
    inputRDD = sc.parallelize(input)
    inputDataframe = sqlcontext.createDataFrame(inputRDD, schema)
    vectorAssembler = VectorAssembler().setInputCols(schema).setOutputCol('features')
    inputDF = vectorAssembler.transform(inputDataframe).select('features')
    resultDict =  trainedModel.transform(inputDF).select("prediction").collect()[0].asDict()
    return resultDict['prediction']
