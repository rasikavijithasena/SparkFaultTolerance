# SparkFaultTolerance

First start flume-agent which is in doc folder. Use following format.
$ flume-ng agent -c pathToAgent -f agentConfigurationFile -n agentName

Then run following command. 
$ mvn package 

Then after, go to target/appassembler/bin folder and execute following command. Then program will execute and directory will be updated. 
$ ./update_file
$ ./spark_checkpoints
