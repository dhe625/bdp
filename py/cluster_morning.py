from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator

if __name__ == "__main__":
	spark = SparkSession.builder.appName("kmeans").getOrCreate()
	df = spark.read.load("hdfs:///user/maria_dev/projectData/morning_data/part-00000-74d3359d-c0b9-495b-b498-95a102618c7c-c000.csv", format="csv", sep=",", inferSchema="true", header="true")
	
	vecAssembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
	new_df = vecAssembler.transform(df)
	
	silhouette_scores=[]
	evaluator = ClusteringEvaluator(featuresCol='features', metricName='silhouette')
	for K in range(2,15):
		KMeans_=KMeans(featuresCol='features', k=K)
		KMeans_fit = KMeans_.fit(new_df)
		KMeans_transform=KMeans_fit.transform(new_df)
		evaluation_score=evaluator.evaluate(KMeans_transform)
		silhouette_scores.append(evaluation_score)
	
	for i in range(len(silhouette_scores)):
		print(i+2, "clusters:", silhouette_scores[i])
	
	kmeans = KMeans(k=12, seed=1)
	model = kmeans.fit(new_df.select('features'))

	centers = model.clusterCenters()
	
	sc = spark.sparkContext
	centroids = sc.parallelize(centers).map(lambda x: [float(i) for i in x]).toDF(["latitude", "longitude"])
	
	centroids.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/centroids_morning")
	
	print("Clusters of morning")
	
	transformed = model.transform(new_df)

	result0 = transformed.filter("prediction == 0").select('longitude','latitude')
	result1 = transformed.filter("prediction == 1").select('longitude','latitude')
	result2 = transformed.filter("prediction == 2").select('longitude','latitude')
	result3 = transformed.filter("prediction == 3").select('longitude','latitude')
	result4 = transformed.filter("prediction == 4").select('longitude','latitude')
	result5 = transformed.filter("prediction == 5").select('longitude','latitude')
	result6 = transformed.filter("prediction == 6").select('longitude','latitude')
	result7 = transformed.filter("prediction == 7").select('longitude','latitude')
	result8 = transformed.filter("prediction == 8").select('longitude','latitude')
	result9 = transformed.filter("prediction == 9").select('longitude','latitude')
	result10 = transformed.filter("prediction == 10").select('longitude','latitude')
	result11 = transformed.filter("prediction == 11").select('longitude','latitude')

	result0.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/0_morning")
	result1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/1_morning")
	result2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/2_morning")
	result3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/3_morning")
	result4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/4_morning")
	result5.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/5_morning")
	result6.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/6_morning")
	result7.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/7_morning")
	result8.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/8_morning")
	result9.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/9_morning")
	result10.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/10_morning")
	result11.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clustered_morning/11_morning")
