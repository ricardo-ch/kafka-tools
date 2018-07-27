#!/usr/bin/env bash

MOCKERY_PATH :=  $(shell  [ -z "$${GOBIN}" ] && echo $${GOPATH}/bin/mockery ||  echo $${GOBIN}/mockery; )

.PHONY: build
build:
	CGO_ENABLED=0 go build -o ./ktbox -a -ldflags '-s' -installsuffix cgo main.go

.PHONY: build-all
build-all:
	GOOS=linux CGO_ENABLED=0 go build -o ./bin/ktbox -a -ldflags '-s' -installsuffix cgo main.go
	GOOS=darwin CGO_ENABLED=0 go build -o ./bin/ktbox_mac -a -ldflags '-s' -installsuffix cgo main.go
	GOOS=windows CGO_ENABLED=0 go build -o ./bin/ktbox.exe -a -ldflags '-s' -installsuffix cgo main.go
	chmod +x ./bin/ktbox
	chmod +x ./bin/ktbox_mac


.PHONY: rundep
rundep:
	docker kill testkafka
	docker rm testkafka
	docker run -d -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=kafka -e SAMPLEDATA=0 --name testkafka landoop/fast-data-dev:1.1
	sleep 10
	kafka-topics.sh --zookeeper local:2181 --topic test-topic --create --partitions 2 --replication-factor 1
	kafka-console-producer.sh --broker-list kafka:9092 --topic test-topic --property "parse.key=true" --property "key.separator=#"  < input-test
	kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-topic --from-beginning --timeout-ms=2000 --consumer-property group.id=testcg
	kafka-console-producer.sh --broker-list kafka:9092 --topic test-topic --property "parse.key=true" --property "key.separator=#"  < input-test


.PHONY: mockkafka
mockkafka:
	go get -u github.com/vektra/mockery/...
	${MOCKERY_PATH} -case "underscore" -dir $$GOPATH/src/github.com/Shopify/sarama -output ./lib/mocks -name="(Client)|(AsyncProducer)|(Consumer)|(OffsetManager)|(PartitionConsumer)|(PartitionOffsetManager)"
