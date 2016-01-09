    java -jar kafka-offset-monitor.jar \
      --logstashHost LOGSTASH-HOST \
      --logstashPort 4561 \
      --zkHosts ZOOKEEPER-HOSTS \
      --spoutId StormSpout_TWITTER_RECORDS
    
    ZOOKEEPER-HOSTS:
    host:port[,host:port]
    example: 10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181
