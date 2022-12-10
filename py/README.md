
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
