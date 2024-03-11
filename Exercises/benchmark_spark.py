from time import time
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

i_time = time()
to_remove = [',',':',';','.','!','?']

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

text = sc.textFile('file:///home/user/Downloads/romeo_and_juliet.txt')
words = text.flatMap(lambda row: row.split(' '))
words_filtered = words.filter(lambda word: False if len(word) == 0 or word[-1] in to_remove else True)
word_count = words_filtered.map(lambda word: (word,1))
word_counter = word_count.reduceByKey(lambda x,y: x+y)
frequent_words = word_counter.filter(lambda t: t[1] > 5)

f_time = time()
print(f'Done with PySpark in {f_time-i_time} s.')
print(f'REMEMBER: do not shoot an ant with a cannon ;)')