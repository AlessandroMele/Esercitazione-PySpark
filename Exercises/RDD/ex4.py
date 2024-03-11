# 4 - Filtrare le parole con un numero di caratteri > 3 e < 10 e mostrare le prime 5 rimanenti

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

text = sc.textFile('file:///home/user/Downloads/romeo_and_juliet.txt')
words = text.flatMap(lambda row: row.split(' '))
filtered_words = words.filter(lambda word: len(word) > 3 and len(word) < 10)

print(filtered_words.take(5))
print(f'Removed {words.count()-filtered_words.count()} words.')
