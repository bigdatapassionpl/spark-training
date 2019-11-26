
# Uruchomienie jar'a

~~~
export SPARK_MAJOR_VERSION=2

spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class com.bigdatapassion.spark.core.MovieGenres $HADOOP_PROJECT/spark/spark-core/target/spark-core-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class com.bigdatapassion.spark.core.SparkPairRddBasic $HADOOP_PROJECT/spark/spark-core/target/spark-core-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class com.bigdatapassion.spark.core.SparkRddBasic $HADOOP_PROJECT/spark/spark-core/target/spark-core-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-submit --master yarn --deploy-mode cluster --num-executors 3 --class com.bigdatapassion.spark.core.WordCount $HADOOP_PROJECT/spark/spark-core/target/spark-core-1.0-SNAPSHOT-jar-with-dependencies.jar

hdfs dfs -ls $HADOOP_HDFS_HOME/wyniki/spark/
hdfs dfs -cat $HADOOP_HDFS_HOME/wyniki/spark/part-00000
~~~