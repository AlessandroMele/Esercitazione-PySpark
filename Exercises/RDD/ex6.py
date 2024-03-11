# 6 - Visualizzare le occorrenze delle parole che non terminano con caratteri speciali e si ripetono almeno 5 volte

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

to_remove = [',',':',';','.','!','?','/']
text = sc.textFile('file://home/user/Downloads/romeo_and_juliet.txt')

words = text.flatMap(lambda row: row.split(' '))
words_filtered = words.filter(lambda word: len(word) != 0 and word[-1] not in to_remove)
word_count = words_filtered.map(lambda word: (word,1))
word_counter = word_count.reduceByKey(lambda x,y: x+y)
frequent_words = word_counter.filter(lambda t: t[1] > 5)

print(frequent_words.collect())
