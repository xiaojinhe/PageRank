PageRank Algorithm Using Apache Hadoop Framework

To Execute the program:
1) Install Hadoop
2) Pick a dataset from http://snap.stanford.edu/data/#web or others with the input format: sourceNode \t distNode
3) Put the dataset file into Hadoop hdfs 
4) Compile all the source codes and build a JAR
5) Run the algorithm using Hadoop with five arguments (e.g. hadoop jar pr.jar Driver args1 args2 args3 args4 args5(optional))
- args1: the directory of a dataset file in Hadoop hdfs
- args2: the directory of transition matrix and PageRank files that you want to be saved
- args3: the directory of UnitMultiplication result
- args4: the number of iterations of convergence
- args5: the damping factor (optional, default = 0.85)
6) The transition matrix file and PageRank output for each iteration will be found under the directory of args2/transtion and
   args2/pagerank + i (i-th iteration). The final PageRank output result presenting in descending order will be found under
   the directory of args2/PageRankOrder.
   
