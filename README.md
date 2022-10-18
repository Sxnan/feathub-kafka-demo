# Feathub Kafka Demo

This demo shows an end to end example of using Feathub to define and compute features
with Flink.

The Feathub job read the input data from Kafka topics, computes the features, and output
the feature to a Kafka topic.

## Inputs Format

In this demo, features are computed with two inputs, the user_events and the
item_events.

### user_click_events

The user events are generated when a user click on an item.

```
{
    "user_id":"1234",
    "location":"hangzhou",
    "item_id":"1234",
    "device":"iphone",
    "ts":"2022-01-01 00:00:00"   
}
```

### item_event

The item events are generated when the state of an item is changed, e.g. the item is
changed to online or offline.

```
{
    "item_id":"1234",
    "state":"online", 
    "primary_category":"agriculture",
    "secondary_category":"melon",  
    "province_id":"zhejiang", 
    "city_id":"hangzhou",
    "ts":"2022-01-01 00:00:00"
}
```

## Output Features

- avg_click_interval: Average click interval of the last 20 clicks of a user in an hour
- secondary_category_value_counts: Counts of each secondary category of the last 20
  clicks of a user in an hour

Example of an output:

```
{
    "user_id":"1",
    "ts":"2022-01-01 02:49:59",
    "secondary_category_value_counts": {
        "banana":1,
        "apple":1
    },
    "avg_click_interval":900.0
}
```

## Step

1. Install Feathub
   ```bash
   # Install nightly build of Feathub
   $ pip install feathub-nightly
   ```
2. Download Flink 1.15.2
    ```bash
    $ curl -LO https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz
    $ tar -xzf flink-1.15.2-bin-scala_2.12.tgz
    ```
3. Set up the environment with `docker-compose`. Use the following command to start the
   Kafka and standalone Flink cluster.
    ```bash
    $ docker-compose up -d
    ```
   After Flink cluster started, you should be able to navigate to the web UI at 
   [localhost:8081](http://localhost:8081) to view the Flink dashboard.
4. Prepare the data to Kafka with the following command:
    ```bash
    $ python3 python/prepare_data.py
    ```
5. Run the following command to package the code to submit to the standalone
    ```bash
    $ cd python; zip -r demo.zip **/*.py; mv demo.zip ..; cd ..
    ```
6. Submit the job with the following command:
    ```bash
    $ ./flink-1.15.2/bin/flink run --pyModule compute_feature --pyFiles demo.zip --detach
    ```

## Result

After the job starts running, you can check the result in the Kafka topic

```bash
$ curl -LO https://downloads.apache.org/kafka/3.2.3/kafka_2.12-3.2.3.tgz
$ tar -xzf kafka_2.12-3.2.3.tgz
$ ./kafka_2.12-3.2.3/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9093 \
    --topic user_click_features \
    --from-beginning
```

## Cleanup

When you finish, you can tear down the environment with the following command:

```bash
docker-compose down
```