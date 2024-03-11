# 1 - Visualizzare per ogni comune la relativa regione di appartenenza

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df_m = spark.read.options(header='True', delimiter=',').csv('file:///home/user/Downloads/municipalities.csv')
df_p = spark.read.options(header='True', delimiter=',').csv('file:///home/user/Downloads/provinces.csv')
df_j = df_m.join(df_p, df_m['province'] == df_p['city'], 'inner')

df_j['municipality','region'].sort('municipality','region').show()
