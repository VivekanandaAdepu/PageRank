hadoop fs -rm -r /user/cloudera/pagerank/outpu*
hadoop jar PageRank.jar PageRank /user/cloudera/pagerank/input/wiki-micro.txt /user/cloudera/pagerank/output

