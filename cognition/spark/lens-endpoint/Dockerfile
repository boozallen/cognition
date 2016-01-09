#run this from the lens-endpoint folder

FROM java:8-jre 

ADD target/lens-endpoint-2.0.0-dist.tar.gz /opt/lens/
WORKDIR /opt/lens
EXPOSE 7080
CMD java -cp config/lens-api.properties:lib/lens-endpoint-2.0.0.jar com.boozallen.cognition.endpoint.applications.DataService server config/config.yml


#docker run -it -v [local path to config.yml]:/opt/lens/config/config.yml -v [local path to lens-api.properties]:/opt/lens/config/lens-api.properties -p 8080:7080 cognition/lens
