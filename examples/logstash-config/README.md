# Logstash Configuration for Log Monitoring

Cognition uses ELK (Elasticsearch, Logstash & Kibana) stack for collecting and monitoring logs from each component. Log lines are sent to Logstash from Log4j or Logback, depending on component.

## Logstash Configuration

Logstash has built-in support for Log4j input. For Logback, we enable tcp input for accepting JSONs.

    input {
      log4j {
        port => 4560
      }
      tcp {
        port => 4561
        codec => "json"
      }
    }

## Log4j Configuration

Using Log4j's built-in SocketAppender, send log lines to Logstash, specifying component name:

    log4j.appender.logstash=org.apache.log4j.net.SocketAppender
    log4j.appender.logstash.remoteHost=LOGSTASH-HOST
    log4j.appender.logstash.port=4560
    log4j.appender.logstash.application=accumulo

## Logback Configuration

Logstash released LogstashEncoder and LogstashTcpSocketAppender for sending log lines from Logback, which requires a recent version of Logback. Depending on component (i.e. Storm), it may be necessary to remove existing Logback JARs, and install newer version.
  
      <appender name="logstash" class="net.logstash.logback.appender.LogstashTcpSocketAppender">
        <remoteHost>LOGSTASH-HOST</remoteHost>
        <port>4561</port>
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
          <fieldNames>
            <level>priority</level>
            <thread_name>thread</thread_name>
          </fieldNames>
          <customFields>{"application":"storm"}</customFields>
        </encoder>
      </appender>