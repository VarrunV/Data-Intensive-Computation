package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text 		PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it
    // set local[2] because n of streaming sources is 1
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaStream")

    // define spark streaming context
    val ssc = new StreamingContext(conf, Seconds(2)) 
    ssc.checkpoint("checkpoint")
    
    val kafkaConf = Map[String, String]("metadata.broker.list" -> "localhost:9092", "zookeeper.connect" -> "localhost:2181", "group.id" -> "kafka-spark-streaming", "zookeeper.connection.timeout.ms" -> "1000")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("avg"))
    
    // get record
    val value = messages.map{case (key, value) => value.split(',')}
    // get pairs
    val pairs = value.map{record => (record(1), record(2).toDouble)}

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Tuple2[Double, Int]]): (String, Double) = {
        val oldState:Tuple2[Double, Int] = state.getOption.getOrElse((0.0, 0))
	val sum:Double = oldState._1
	val count:Int = oldState._2
        val newSum = value.getOrElse(0.0) + sum
        val newCount = count + 1
        state.update((newSum, newCount))
        (key, newSum/newCount)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
  }
}
