# 1 - Contare il numero di caratteri in un file di testo

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

text = sc.textFile('file:///home/user/Downloads/romeo_and_juliet.txt')
lengths = text.map(lambda s:len(s))
total = lengths.reduce(lambda a,b: a+b)
print(total)