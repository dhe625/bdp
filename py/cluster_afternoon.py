from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":
	spark = SparkSession.builder.appName("kmeans").getOrCreate()
	df = spark.read.load("hdfs:///user/maria_dev/projectData/afternoon_data/part-00000-69ea15eb-2e6e-4154-b9a1-849f1f4db304-c000.csv", format="csv", sep=",", inferSchema="true", header="true")
	
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
	
	result1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/1_afternoon")
	result2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/2_afternoon")
	result3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/3_afternoon")
	result4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/4_afternoon")
	result5.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/5_afternoon")
	result6.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/6_afternoon")
	result7.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/7_afternoon")
	result8.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/8_afternoon")
	result9.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/9_afternoon")
	result0.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/0_afternoon")
	
