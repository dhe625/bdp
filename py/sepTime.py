from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import functools

def unionAll(dfs):
	    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

if __name__ == "__main__":
	spark = SparkSession.builder.appName("2021_01.csv").getOrCreate()
	df1 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_01.csv", format="csv", sep=",", inferSchema="true", header="true")
	df2 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_02.csv", format="csv", sep=",", inferSchema="true", header="true")
	df3 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_03.csv", format="csv", sep=",", inferSchema="true", header="true")
	df4 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_04.csv", format="csv", sep=",", inferSchema="true", header="true")
	df5 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_05.csv", format="csv", sep=",", inferSchema="true", header="true")
	df6 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_06.csv", format="csv", sep=",", inferSchema="true", header="true")
	df7 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_07.csv", format="csv", sep=",", inferSchema="true", header="true")
	df8 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_08.csv", format="csv", sep=",", inferSchema="true", header="true")
	df9 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_09.csv", format="csv", sep=",", inferSchema="true", header="true")
	df10 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_10.csv", format="csv", sep=",", inferSchema="true", header="true")
	df11 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_11.csv", format="csv", sep=",", inferSchema="true", header="true")
	df12 = spark.read.load("hdfs:///user/maria_dev/projectData/2021_12.csv", format="csv", sep=",", inferSchema="true", header="true")
	
	df = unionAll([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12])

	morning = df.filter(F.hour(F.col("time")) < 12).show()
	afternoon = df.filter(F.hour(F.col("time")) >= 12).show()
	
	morning.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/morning_data.    csv")
	afternoon.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/afternoon_data.csv")

