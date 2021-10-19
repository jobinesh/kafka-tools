# kafka docker compose
You can quickly set up the Kafka cluster for development using the docker-compose.yml available in the current directory. To learn more, refer to this article: https://www.baeldung.com/ops/kafka-docker-setup  


To start a single-node Kafka broker setup cluster:  
```
docker-compose -f docker-compose.yml up
```
To stop the Kafka cluster:  
```
docker-compose -f docker-compose.yml down
```
