# /!\ Not ready yet. Do not use it

# How to check output at each step

```
topicSource=[YOUR_TOPIC]
brokers=[BROKERS]

for i in {0..9}
do
	kafkacat -C -b $brokers -t kafka-hitman-work -o beginning -e -f "%k#%s\n" -p $i > wtopic${i}.txt
	kafkacat -C -b $brokers -t $topicSource -o beginning -e -f "%k#%s\n" -p $i > stopic${i}.txt
	diff wtopic${i}.txt stopic${i}.txt
done
```