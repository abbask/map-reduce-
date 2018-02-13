#!/bin/sh
echo "--------------------------- Starting MapReduce job on both datasets ----------------------------\n"
cd ~/p1/src/part3/
sudo rm -rf tempcalc3_classes/*
sudo rm tempcalc3.jar
sudo rm -rf ~/p1/output/part3/output1 ~/p1/output/part3/output2
sudo mkdir ~/p1/output/part3/output1
sudo mkdir ~/p1/output/part3/output2
sudo javac -classpath /usr/local/hadoop/hadoop-1.0.4/hadoop-core-1.0.4.jar -d tempcalc3_classes/ TempCalc3.java
sudo jar -cvf tempcalc3.jar -C tempcalc3_classes/ .
cd /usr/local/hadoop/hadoop-1.0.4/
bin/hadoop dfs -rmr /user/hduser/temp/p1/temp1/
bin/hadoop dfs -rmr /user/hduser/temp/p1/temp2/
bin/hadoop dfs -rmr /user/hduser/output/p1/output1/
bin/hadoop dfs -rmr /user/hduser/output/p1/output2/
echo "********************************* GROUP10 JOB: MaxCalc on dataset1 *****************************\n"
bin/hadoop jar ~/p1/src/part3/tempcalc3.jar TempCalc3 /user/hduser/input/p1/input1 /user/hduser/temp/p1/temp1 /user/hduser/output/p1/output1
sudo bin/hadoop dfs -copyToLocal /user/hduser/output/p1/output1/* ~/p1/output/part3/output1
echo "********************************* GROUP10 JOB: MaxCalc on dataset2 *****************************\n"
bin/hadoop jar ~/p1/src/part3/tempcalc3.jar TempCalc3 /user/hduser/input/p1/input2 /user/hduser/temp/p1/temp2 /user/hduser/output/p1/output2
sudo bin/hadoop dfs -copyToLocal /user/hduser/output/p1/output2/* ~/p1/output/part3/output2
