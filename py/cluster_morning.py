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
	
	result1 = transformed.filter("prediction == 1").select('longitude','latitude')
	result2 = transformed.filter("prediction == 2").select('longitude','latitude')
	result3 = transformed.filter("prediction == 3").select('longitude','latitude')
	result4 = transformed.filter("prediction == 4").select('longitude','latitude')
	result5 = transformed.filter("prediction == 5").select('longitude','latitude')
	result6 = transformed.filter("prediction == 6").select('longitude','latitude')
	result7 = transformed.filter("prediction == 7").select('longitude','latitude')
	result8 = transformed.filter("prediction == 8").select('longitude','latitude')
	result9 = transformed.filter("prediction == 9").select('longitude','latitude')
	result0 = transformed.filter("prediction == 0").select('longitude','latitude')
	
	result1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/1_morning")
	result2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/2_morning")
	result3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/3_morning")
	result4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/4_morning")
	result5.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/5_morning")
	result6.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/6_morning")
	result7.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/7_morning")
	result8.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/8_morning")
	result9.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/9_morning")
	result0.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/0_morning")
	
