package org.kafka.cli.commands

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.kafka.cli.{CommandLineAction, CommandLineActionFactory}
import org.kafka.cli.utils.TryWithClosable
import scopt.{OptionParser, RenderingMode}
import scala.collection.JavaConversions._

/**
  * @author menshin on 4/3/17.
  */
class SeekToBeginAction(val config: SeekToBeginActionConfig) extends CommandLineAction with TryWithClosable {

  private val MaxWaitMs = 1000

  override def perform(): Unit = {
    tryWith(new KafkaConsumer[String, String](createConsumerConfig(config.brokerList, config.groupId))) {
      consumer => {
        Option.apply(consumer.partitionsFor(config.topic)) match {
          case Some(partitions) =>
            partitions
              .filter(pi => config.partitions.isEmpty || config.partitions.contains(pi.partition()))
              .foreach(pi => {
                val topicPartition = new TopicPartition(pi.topic(), pi.partition())

                consumer.assign(List(topicPartition))
                consumer.seekToBeginning()
                val firstOffset = consumer.position(topicPartition)
                consumer.commitSync()

                println(s"reset offset ${pi.topic}:${pi.partition} to $firstOffset")
              })
            consumer.unsubscribe()
          case None =>
            println(s"failed to find topic ${config.topic}")
        }
      }
    }

  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }


}


private[commands] case class SeekToBeginActionConfig(brokerList: String = null,
                                                     topic: String = null,
                                                     groupId: String = null,
                                                     partitions: Set[Int] = Set.empty)

object SeekToBeginAction extends CommandLineActionFactory {

  val Parser: OptionParser[SeekToBeginActionConfig] = new OptionParser[SeekToBeginActionConfig]("seekToBegin") {
    opt[String]('b', "brokerList").required().action((s, c) =>
      c.copy(brokerList = s)).text("broker servers list")
    opt[String]('t', "topic").required().action((s, c) =>
      c.copy(topic = s)).text("topic name")
    opt[String]('g', "groupId").required().action((s, c) =>
      c.copy(groupId = s)).text("consumer group id")
    opt[String]('p', "partitions").action((s, c) =>
      c.copy(partitions = s.split(",").map(_.toInt).toSet)).text("comma separated list of partitions")
  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {

    Parser.parse(args, SeekToBeginActionConfig()) match {
      case Some(config) => Some(new SeekToBeginAction(config))
      case None => None
    }
  }

  override def renderUsage(mode: RenderingMode): String = Parser.renderUsage(mode)
}