# DSCAN-MR
In this project we propose an implementaion of the structral graph clustering using Mapreduce framework.
@author Wissem Inoubli inoubliwissem@gmail.com
# Required software and libraries
- Java 8 
- maven 3.5 or higher
- Hadoop 2.7 or higher
# Build the jar file
- clone this poject
- cd DSCAN-MR
- ```shell
      $ mvn package
   ```
# Start the hadoop cluster
  ```shell
      $ $HADOOP_HOME/sbin/start-all.sh
   ```
# Input data
The input graph must be list of edges like the following example:
#vertices are separeted by a tabulation  
1 2
3 3
1 4
# Run the proposed algorithm
## the program needs 5 required parameters
1. Path to input data (I)
2. Path to final resuls (O)
3. Path to a temporal directory(T)
4. Number of the mapper(M)
5. Number of reducer (R)

## run the programm
  ``` shell
      $ yarn jar [DSCAN.jar] com.DSCAN.Hadoop.GlobalPipe  hdfs://master:9000/I hdfs://master:9000/O hdfs://master:9000/T 2 3
   ```
