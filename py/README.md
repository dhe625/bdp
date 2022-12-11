[ 코드 설명 ]

본 코드는 PySpark을 이용하는 코드이다.

[ 데이터 전처리 코드 ]

<img width="1420" alt="스크린샷 2022-12-10 오후 6 00 12" src="https://user-images.githubusercontent.com/100830963/206842547-02e00608-c393-4ddf-9a01-c424a94d3809.png">

unionAll 함수를 정의하여 각 월의 데이터를 하나로 통합한다.

12시를 기준으로 00:00 ~ 11:59 까지 morning, 12:00 ~ 23:59 까지 afternoon으로 나눈다.

각각 DataFrame을 CSV로 변환하여 hdfs에 저장한다.

[ 데이터 분석 코드 ]

<img width="884" alt="code1" src="https://user-images.githubusercontent.com/100830963/206903865-0a09790e-7679-42e0-aafa-8eea2be46849.png">

최적의 위치를 찾기 위해 K-means clustering을 이용한다.

위에서 저장된 afternoon CSV를 기준으로 필요한 attribute인 'latitude'와 'longitude'만 추출하여 vector로 변환한다.

cluster를 나타내는 'features' attribute를 생성한다.

최적의 cluster 갯수를 찾기 위해 silhouette 계수를 비교한다.

<img width="286" alt="afternoon" src="https://user-images.githubusercontent.com/100830963/206903771-a2b03347-9f99-4350-a3dd-fc067f9d7a3d.png">

cluster가 13개일 때 최적인 것을 확인할 수 있다.

따라서 cluster 갯수를 13으로 설정한다.

<img width="887" alt="code2" src="https://user-images.githubusercontent.com/100830963/206904025-eaf13913-842e-46ff-bcb7-3eff549da593.png">

각 cluster 별 centroids를 DataFrame으로 만들고 CSV로 변환하여 hdfs에 저장한다.

<img width="414" alt="code3" src="https://user-images.githubusercontent.com/100830963/206904000-3ff34e0a-c011-41f4-928d-6b399320e525.png">

clustered data를 출력한다.

<img width="958" alt="clustered_afternoon" src="https://user-images.githubusercontent.com/100830963/206904118-2360419a-67ce-485b-bf6f-78582a7f9153.png">

각 cluster 별로 추출하여 CSV로 변환하여 hdfs에 저장한다.

morning CSV 역시 같은 작업을 수행한다.

[ 참고 사이트 ]

1. https://spark.apache.org/docs/latest/ml-clustering.html#k-means
2. https://stackoverflow.com/questions/64563540/how-do-i-convert-a-numpy-array-to-a-pyspark-dataframe
3. https://www.data4v.com/tutorial-k-means-clustering-on-spark/
4. https://scikit-learn.org/stable/auto_examples/cluster/plot_kmeans_silhouette_analysis.html
