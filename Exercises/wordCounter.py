from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import sys

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def counter(in_path, out_path):
	file = sc.textFile(in_path)
	words = file.flatMap(lambda x: x.split(' '))
	w_count = words.map(lambda x: (x,1))
	word_counter = w_count.reduceByKey(lambda a, b: a+b)

	with open(out_path, 'w') as file:
	  for row in word_counter.collect():
	   file.write(f'{row[0]}: {row[1]}\n')
   
   
if __name__ == '__main__':
	input_path = str(sys.argv[1]) 
	output_path = str(sys.argv[2]) 
	counter(input_path, output_path)
	