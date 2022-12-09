from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":
	spark = SparkSession.builder.appName("kmeans").getOrCreate()
	df = spark.read.load("hdfs:///user/maria_dev/projectData/morning_data/part-00000-74d3359d-c0b9-495b-b498-95a102618c7c-c000.csv", format="csv", sep=",", inferSchema="true", header="true")
	
	vecAssembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
	new_df = vecAssembler.transform(df)
	
	kmeans = KMeans(k=10, seed=1)
	model = kmeans.fit(new_df.select('features'))

	transformed = model.transform(new_df)

	result = transformed.filter("prediction == 9").select('longitude','latitude')
	
	result.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/test3")
