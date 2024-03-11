# 2 - Visualizzare il numero medio di caratteri per riga di un file di testo

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

text = sc.textFile('file:///home/user/Downloads/romeo_and_juliet.txt')
lengths = text.map(lambda s:len(s))
total_lengths = lengths.reduce(lambda a,b: a+b)
total_lines = text.count()
print(total_lengths/total_lines)