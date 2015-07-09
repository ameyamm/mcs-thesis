#!/bin/sh
PROJ_DIR=$HOME/git/mcs-thesis/ghsom
export SPARK_LOCAL_IP=localhost
spark-submit --class com.ameyamm.mcs_thesis.input_generator.ElectionDatasetReader \
--master "local[4]" \
$PROJ_DIR/target/ghsom-0.0.1-jar-with-dependencies.jar
