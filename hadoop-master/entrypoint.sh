#!/bin/bash

# Format the HDFS if necessary
if [ ! -d "/usr/local/hadoop/data/namenode" ]; then
  $HADOOP_HOME/bin/hdfs namenode -format
fi

# Start the HDFS
$HADOOP_HOME/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
$HADOOP_HOME/sbin/hadoop-daemons.sh --config $HADOOP_CONF_DIR --script hdfs start datanode

# Start the YARN
$HADOOP_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager
$HADOOP_HOME/sbin/yarn-daemons.sh --config $HADOOP_CONF_DIR start nodemanager

# Keep the container running by tailing the logs
tail -f $HADOOP_HOME/logs/*
