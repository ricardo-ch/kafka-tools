.PHONY: rundep
rundep:
	docker kill testkafka
	docker rm testkafka
	docker run -d -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=kafka -e SAMPLEDATA=0 --name testkafka landoop/fast-data-dev:1.1
	sleep 10
	kafka-topics.sh --zookeeper zookeeper:2181 --topic test-francois --create --partitions 2 --replication-factor 1
	kafka-console-producer.sh --broker-list kafka:9092 --topic test-francois --property "parse.key=true" --property "key.separator=#"  < input-test
	kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-francois --from-beginning --timeout-ms=2000 --consumer-property group.id=testcg
	kafka-console-producer.sh --broker-list kafka:9092 --topic test-francois --property "parse.key=true" --property "key.separator=#"  < input-test


.PHONY: mockkafka
mockkafka:
	 mockery -case "underscore" -dir $$GOPATH/src/github.com/Shopify/sarama -output ./lib/mocks -name="(Client)|(AsyncProducer)|(Consumer)|(OffsetManager)|(PartitionConsumer)|(PartitionOffsetManager)"