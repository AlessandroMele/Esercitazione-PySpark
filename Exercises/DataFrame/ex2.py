# 2 - Visualizzare la regione con il maggior numero di comuni

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df_m = spark.read.options(header='True', delimiter=',').csv('file:///home/user/Downloads/municipalities.csv')
df_p = spark.read.options(header='True', delimiter=',').csv('file:///home/user/Downloads/provinces.csv')
df_j = df_m.join(df_p, df_m['province'] == df_p['city'], 'inner')
df_c = df_j.groupBy('region').agg(count('*').alias('n_municipalities'))

df_c.sort('n_municipalities', ascending=False).show(1)
