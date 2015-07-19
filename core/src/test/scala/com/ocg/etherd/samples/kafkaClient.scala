package com.ocg.etherd.samples

import java.util.Properties
import java.util.concurrent.Executors

import kafka.consumer.{ConsumerConfig, KafkaStream}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer._

import scala.collection.immutable.List
import scala.collection.mutable.Map

object KafkaClient {
  def main(args: Array[String]): Unit ={
    new KafkaStreamReader(1, "")
    println("Consumer: Done")
  }

  def send() {
    println("inside main")
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

    val k = "key1".getBytes
    val v = "val1".getBytes
    val ft = producer.send(new ProducerRecord("test", 0, k, v), new Callback(){
      def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
        println(e.toString)
      }
    })
    val rm = ft.get
    println(rm.offset)
  }

  object KafkaProperties {
    final val zkConnect: String = "127.0.0.1:2181"
    final val groupId: String = "group1"
    final val topic: String = "test"
    final val kafkaServerURL: String = "localhost"
    final val kafkaServerPort: Int = 9092
    final val kafkaProducerBufferSize: Int = 64 * 1024
    final val connectionTimeOut: Int = 100000
    final val reconnectInterval: Int = 10000
    final val topic2: String = "topic2"
    final val topic3: String = "topic3"
    final val clientId: String = "SimpleConsumerDemoClient"
  }

  class PartitonReceiver(stream: KafkaStream[Array[Byte],Array[Byte]], partition: Int) extends Runnable {

    def run(): Unit = {
      println("Called runnable for partition %d".format(this.partition))
      for(elem <- this.stream.iterator()) {
        println("Message is %s".format(new StringDecoder().fromBytes(elem.message())))
      }
    }
  }

  class KafkaStreamReader(partitions: Int, id: String) {
    val executorService = Executors.newFixedThreadPool(partitions)
    private val consumerConnect = kafka.consumer.Consumer.create(this.createConsumerConfig)
    val topicPatritionsMap = Map(("test", partitions))
    val streams = consumerConnect.createMessageStreams(topicPatritionsMap).get("test").get
    this.beginReceiving(streams)

    private def beginReceiving(streams: List[KafkaStream[Array[Byte],Array[Byte]]]): Unit = {
      println("Consumer: Inside begin receive")
      for {pCount <- 0 to streams.size
           stream <- streams} {
        executorService.submit(new PartitonReceiver(stream, pCount))
      }
      println("Consumer: Waiting messages to come")
      Thread.sleep(1200000)
    }
    private def createConsumerConfig: ConsumerConfig = {
      val props: Properties = new Properties
      props.put("zookeeper.connect", KafkaProperties.zkConnect)
      props.put("group.id", KafkaProperties.groupId)
      props.put("zookeeper.session.timeout.ms", "400")
      props.put("zookeeper.sync.time.ms", "200")
      props.put("auto.commit.interval.ms", "1000")
      new ConsumerConfig(props)
    }
  }
}
