

Pallavi Kollipara (s4015344)

Files:

Input files:

noc_region.csv
person_region.csv
person.csv
competitor_event.csv
medal.csv

files:
task1.pig
task2-1.pig
task2-2.pig
task2udf.py

Steps:

1. Open terminal and run ssh jumphost and then create cluster in jumphost. Type the command ./create_cluster.sh. and then ssh into EMR master node.
2. Upload all the 5 csv files and task2udf.py to HDFS in input folder.
3. Upload all pig files to EMR master node.
4. Now run task1.pig, task2-1.pig, task2-2.pig to get desired outputs by using the command "pig task1.pig","pig task2-1.pig", pig task2-2.pig".