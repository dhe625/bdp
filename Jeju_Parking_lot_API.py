#모듈을 추가한다.
import requests
import pandas as pd
#제주도내 주차장 기본정보 API를 받아온다.
url = "http://api.jejuits.go.kr/api/infoParkingInfoList?code=******"
json_obj = requests.get(url).json()
json_obj
#list를 만들어 데이터프레임화 하고 head를 찍어보았다.
list = []
for infos in json_obj['info']:
    if infos.get('id'):
        list.append([infos['name'],infos['addr'],infos['x_crdn'],infos['y_crdn']])
df = pd.DataFrame(list, columns = ['name', 'addr', 'x_crdn', 'y_crdn'])
df.head()
#CSV 파일로 저장한다.
df.to_csv('C:/Deok/Jeju_parkinglot.csv', header=True, index=False)