export SPARK_LOCAL_IP=172.16.0.6 
#JARS="/home/ameya/.m2/repository/com/datastax/spark/spark-cassandra-connector_2.10/1.3.0-M1/spark-cassandra-connector_2.10-1.3.0-M1.jar","/home/ameya/.m2/repository/com/datastax/cassandra/cassandra-driver-core/2.1.5/cassandra-driver-core-2.1.5.jar","/home/ameya/.m2/repository/com/google/collections/google-collections/1.0/google-collections-1.0.jar","/home/ameya/.m2/repository/io/netty/netty/3.8.0.Final/netty-3.8.0.Final.jar","/home/ameya/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar","/home/ameya/.m2/repository/io/dropwizard/metrics/metrics-core/3.1.0/metrics-core-3.1.0.jar","/home/ameya/.m2/repository/org/slf4j/slf4j-api/1.7.10/slf4j-api-1.7.10.jar","/home/ameya/.m2/repository/com/google/collections/google-collections/1.0/google-collections-1.0.jar","/home/ameya/.m2/repository/io/netty/netty/3.8.0.Final/netty-3.8.0.Final.jar","/home/ameya/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar"

spark-submit --class com.ameyamm.mcsthesis.cassandra_load.Loader --master spark://192.168.101.13:7077 --executor-memory 3500m --driver-memory 1800m --conf "spark.driver.host=172.16.0.6" ../../../target/cassandra-load-0.0.1-jar-with-dependencies.jar
