export SPARK_LOCAL_IP=172.16.0.14
spark-submit --class com.ameyamm.mcsthesis.dataset_generator.DatasetGenerator --master spark://192.168.101.13:7077 --conf "spark.driver.host=172.16.0.14" /home/ameya/git/mcs-thesis/dataset_generator/target/dataset_generator-0.0.1-jar-with-dependencies.jar 
