# 3 - Visualizzare il numero medio di comuni per provincia

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count, avg

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df_m = spark.read.options(header='True', delimiter=',').csv('file:///home/user/Downloads/municipalities.csv')
municipality_c = df_m.groupBy('province').agg(count('*').alias('n_municipalities'))
municipalities_avg = municipality_c.agg(avg('n_municipalities').alias('avg_n_municipalities'))

municipalities_avg.show()
