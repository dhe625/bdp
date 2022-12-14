from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class IdCount(MRJob):
	def steps(self):
		return [
				MRStep(mapper = self.map_id_count,
					reducer = self.reduce_id_count)
				]
	def map_id_count(self,_,line):
		id, count = line.split('\t')
		id = re.sub('[-=+,#/\?:^.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·]', ' ',id)
		if id != 'id':
			yield(id,1)
	def reduce_id_count(self,key,values):
		yield key, sum(values)

if __name__ == '__main__':
	IdCount.run()
