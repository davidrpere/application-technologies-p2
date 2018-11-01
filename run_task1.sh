#!/bin/bash

sbin="/opt/hadoop3/sbin"
dfsc="hdfs dfs"
project_root="/home/drodriguez/Projects/application-technologies-p2"
input_data="${project_root}/input/googlebooks-spa-all-1gram-20120701-a"
input_jar="${project_root}/out/artifacts/application_technologies_p2_jar/application-technologies-p2.jar"

$sbin/stop-all.sh
$sbin/stop-dfs.sh
yes | hdfs namenode -format
$sbin/start-dfs.sh
#$sbin/start-all.sh
#$dfsc -mkdir /user
$dfsc -mkdir -p /user/drodriguez
$dfsc -mkdir input
$dfsc -put $input_data input
hadoop jar $input_jar input output 'dfs[a-z.]+'

