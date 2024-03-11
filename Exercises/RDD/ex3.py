# 3 - Contare le occorrenze delle parole in un file di testo

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

text = sc.textFile('file:///home/user/Downloads/romeo_and_juliet.txt')
words = text.flatMap(lambda x: x.split(' '))
word_occ = words.map(lambda x: (x,1))
counter = word_occ.reduceByKey(lambda x,y: x+y)

print(counter.collect())
