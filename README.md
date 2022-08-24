# Flink API Examples for DataStream API and Table API in Scala 3

Flink is now Scala Free. In the 1.15 release, Flink does not expose any specific Scala version.
Users can now choose whatever Scala version they need in their user code, including Scala 3.

This repository is a reimplementation of Timo Walther's [Flink API Examples for DataStream API and Table API](https://github.com/twalthr/flink-api-examples)
examples in Scala 3.
You can watch his talk [Flink's Table & DataStream API: A Perfect Symbiosis](https://youtu.be/vLLn5PxF2Lw) on YouTube which walks through the Java version of this code.

# How to Use This Repository

1. Import this repository into your IDE (preferably IntelliJ IDEA). The project uses the latest Flink 1.15 version.

2. All examples are runnable from the IDE or SBT. You simply need to execute the `main()` method of every example class.
   Command line example:
   ```shell
   sbt "runMain com.ververica.example3"
   ```

3. In order to make the examples run within IntelliJ IDEA, it is necessary to tick
   the `Add dependencies with "provided" scope to classpath` option in the run configuration under `Modify options`.

4. For the Apache Kafka examples, download and unzip [Apache Kafka](https://kafka.apache.org/downloads).

5. Start up Kafka and Zookeeper:
   ```shell
   ./bin/zookeeper-server-start.sh config/zookeeper.properties &

   ./bin/kafka-server-start.sh config/server.properties &
   ```

6. Run `FillKafkaWithCustomers` and `FillKafkaWithTransactions` to create and fill the Kafka topics with Flink.
