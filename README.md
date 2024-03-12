# Esercitazioni PySpark per il corso di Big Data Analytics e Machine Learning A.A. 2023/2024.

>Avviare HDFS<br>
``$HADOOP_HOME/sbin/start-all.sh``<br>

>Verifichiamo se tutto è andato a buon fine con il comando jps<br>
``jps``<br>
Dovreste trovare i processi: NameNode, DataNode, jps, ResourceManager, SecondaryNameNode, NodeManager<br>

>Copiamo il file di testo su HDFS<br>
``hdfs dfs -copyFromLocal /home/user/Downloads/romeo_and_juliet.txt ``<br>
In questo modo, copiamo il file, partendo dalla root di HDFS in /user/user/romeo_and_juliet.txt<br>

``hdfs dfs -copyFromLocal /home/user/Downloads/romeo_and_juliet.txt /``<br>
In questo modo, copiamo il file, partendo direttamente nella root di HDFS in /romeo_and_juliet.txt<br>

Per visualizzare il contenuto di una cartella: ``hdfs dfs -ls <path>``<br>
Per rimuovere un file: ``hdfs dfs -rm <path> ``<br>

>Vedere le opzioni di avvio di PySpark<br>
``pyspark --help``<br>
``pyspark -h``<br>

>Avviamo PySpark<br>
``pyspark --master local|yarn``<br>
Nota: per avviare PySpark sopra Yarn, necessariamente deve essere stato avviato HDFS<br>

>Eseguire un file Python su Spark in cluster/local<br>
``spark-submit --master yarn|local wordCounter.py``<br>