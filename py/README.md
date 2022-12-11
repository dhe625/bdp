
<img width="1420" alt="스크린샷 2022-12-10 오후 6 00 12" src="https://user-images.githubusercontent.com/100830963/206842547-02e00608-c393-4ddf-9a01-c424a94d3809.png">

Pyspark를 이용하는 코드를 작성하였다.

unionAll 함수를 정의하여 각 월의 데이터를 하나로 통합하였다.

12시를 기준으로 00:00 ~ 11:59 까지 morning, 12:00 ~ 23:59 까지 afternoon으로 나누었다.

각각 DataFrame을 CSV로 변환하여 hdfs에 저장하였다.

<img width="1329" alt="스크린샷 2022-12-10 오후 6 01 48" src="https://user-images.githubusercontent.com/100830963/206842717-4401091a-b2e1-43f2-9cb7-77b8196bdbe9.png">

Pyspark를 이용하는 코드를 작성하였다.

K-means clustering을 이용했다.

위에서 저장된 morning csv를 기준으로 필요한 attribute인 'latitude'와 'longitude'만 추출하여 vector로 변환했다.

cluster를 나타내는 'features' attribute가 생성된다.

이상치 탐지를 위해 10개의 centroids를 만들었다.

위 csv에서 10개의 cluster을 찾을 수 있도록 적용했다.

각 군집별로 추출하여 CSV로 변환하여 hdfs에 저장하였다.

참고 사이트 : https://stackoverflow.com/questions/47585723/kmeans-clustering-in-pyspark




from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator

if __name__ == "__main__":
	spark = SparkSession.builder.appName("kmeans").getOrCreate()
	df = spark.read.load("hdfs:///user/maria_dev/projectData/morning_data/part-00000-74d3359d-c0b9-495b-b498-95a102618c7c-c000.csv", format="csv", sep=",", inferSchema="true", header="true")
	
	vecAssembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
	new_df = vecAssembler.transform(df)
	
	kmeans = KMeans(k=10, seed=1)
	model = kmeans.fit(new_df.select('features'))

	transformed = model.transform(new_df)
	
	print("Clusters of morning")
	
	evaluator = ClusteringEvaluator()                                                                            
	silhouette = evaluator.evaluate(transformed)
	print("Silhouette with squared euclidean distance = " + str(silhouette))

	centers = model.clusterCenters()
	
	sc = spark.sparkContext
	centroids = sc.parallelize(centers).map(lambda x: [float(i) for i in x]).toDF(["latitude", "longitude"])
	centroids.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("hdfs:///user/maria_dev/projectData/clusterCentroids_morning")
	
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
