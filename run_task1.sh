#!/bin/bash

sbin="/opt/hadoop3/sbin"
dfsc="hdfs dfs"
project_root="/home/drodriguez/Projects/application-technologies-p2"
input_data="${project_root}/input_1gram/"
input_jar="${project_root}/out/artifacts/application_technologies_p2_task1_jar/application-technologies-p2-task1.jar"

$sbin/stop-all.sh
$sbin/stop-dfs.sh
yes | hdfs namenode -format
$sbin/start-dfs.sh
#$sbin/start-all.sh
#$dfsc -mkdir /user
$dfsc -mkdir -p /user/drodriguez
$dfsc -mkdir input_1gram
$dfsc -put $input_data* input_1gram
hadoop jar $input_jar 'dfs[a-z.]+'

