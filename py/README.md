
<img width="1420" alt="스크린샷 2022-12-10 오후 6 00 12" src="https://user-images.githubusercontent.com/100830963/206842547-02e00608-c393-4ddf-9a01-c424a94d3809.png">

Pyspark를 이용하는 코드를 작성하였다.

unionAll 함수를 정의하여 각 월의 데이터를 하나로 통합하였다.

12시를 기준으로 00:00 ~ 11:59 까지 morning, 12:00 ~ 23:59 까지 afternoon으로 나누었다.

각각 DataFrame을 CSV로 변환하여 hdfs에 저장하였다.

<img width="1371" alt="스크린샷 2022-12-11 오전 11 21 26" src="https://user-images.githubusercontent.com/100830963/206883500-f1e42747-9859-4dc3-87c0-c40a6cf4337e.png">

Pyspark를 이용하는 코드를 작성하였다.

K-means clustering을 이용했다.

위에서 저장된 morning CSV를 기준으로 필요한 attribute인 'latitude'와 'longitude'만 추출하여 vector로 변환했다.

cluster를 나타내는 'features' attribute가 생성된다.

이상치 탐지를 위해 10개의 centroids를 만들었다.

위 CSV에서 10개의 cluster을 찾을 수 있도록 적용했다.

<img width="573" alt="스크린샷 2022-12-11 오전 11 03 12" src="https://user-images.githubusercontent.com/100830963/206883575-bcdc22fa-582c-4539-8a01-bde882eee035.png">

cluster가 잘 형성되었는지 평가하기 위해 silhouette 계수를 사용하여 측정하였다.

<img width="1376" alt="스크린샷 2022-12-11 오전 11 22 00" src="https://user-images.githubusercontent.com/100830963/206883621-6dcd29b0-9c85-44e3-a566-ca23aa6502af.png">

만들어진 centroids를 DataFrame으로 만들고 CSV로 변환하여 hdfs에 저장하였다.

각 cluster 별로 추출하여 CSV로 변환하여 hdfs에 저장하였다.

afternoon 역시 같은 작업을 수행한다.

[ 참고 사이트 ]

1. https://stackoverflow.com/questions/47585723/kmeans-clustering-in-pyspark
2. https://stackoverflow.com/questions/64563540/how-do-i-convert-a-numpy-array-to-a-pyspark-dataframe
