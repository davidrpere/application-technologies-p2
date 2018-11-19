#!/bin/bash

sbin="/opt/hadoop3/sbin"
dfsc="hdfs dfs"
project_root="/home/drodriguez/Projects/application-technologies-p2"
input_data1="${project_root}/input_1gram/"
input_data2="${project_root}/input_2gram/"
input_jar="${project_root}/out/artifacts/application_technologies_p2_task2_jar/application-technologies-p2.jar"

$sbin/stop-all.sh
$sbin/stop-dfs.sh
yes | hdfs namenode -format
$sbin/start-dfs.sh


$dfsc -mkdir -p /user/drodriguez
$dfsc -mkdir input_1gram
$dfsc -mkdir input_2gram
$dfsc -put $input_data1* input_1gram
$dfsc -put $input_data2* input_2gram
echo "hadoop jar $input_jar 'character' output_task2 'dfs[a-z.]+'"
