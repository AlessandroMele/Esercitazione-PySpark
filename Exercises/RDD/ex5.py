# 5 - Mostrare le righe che contengono almeno cinque parole

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

text = sc.textFile('file:///home/user/Downloads/romeo_and_juliet.txt')
counter = text.map(lambda row: (row, len(row.split(' '))))
filtered_counter = counter.filter(lambda t: t[1] > 5)

print(filtered_counter.collect())
