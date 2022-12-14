#필요한 클래스 받아오기
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class IdCount(MRJob):
	#map/reduce 인스턴스 만들기
	def steps(self):
		return [
				MRStep(mapper = self.map_id_count,
					reducer = self.reduce_id_count)
				]

	#oid count map 수행 및 id값 중 특수문자 제거
	def map_id_count(self,_,line):
		id, collection_dt, longitude, latitude, time, diff = line.split(',')
		id = re.sub('[-=+,#/\?:^.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·]', ' ',id)
		if id != 'id':
			yield(id,1)

	#oid count reduce 수행
	def reduce_id_count(self,key,values):
		yield (key, sum(values))

if __name__ == '__main__':
	IdCount.run()
