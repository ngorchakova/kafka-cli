package org.kafka.cli.commands

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.kafka.cli.utils.TryWithClosable
import org.kafka.cli.{CommandLineAction, CommandLineActionFactory}
import scopt.{OptionParser, RenderingMode}

import scala.collection.JavaConversions._

/**
  * @author Natalia Gorchakova
  * @since 08.01.2017
  */
class GetOffsetsAction(val config: GetOffsetsActionConfig) extends CommandLineAction with TryWithClosable {

  private val GroupID = "GetOffsetsAction"
  private val MaxWaitMs = 1000

  override def perform(): Unit = {

    tryWith(new KafkaConsumer[String, String](createConsumerConfig(config.brokerList))) {
      consumer => {
        consumer.partitionsFor(config.topic).foreach(pi => {
          val topicPartition = new TopicPartition(pi.topic(), pi.partition())

          consumer.assign(List(topicPartition))
          consumer.seekToBeginning()
          val firstOffset = consumer.position(topicPartition)

          consumer.seekToEnd()
          val lastOffset = consumer.position(topicPartition)

          println(s"${pi.topic}:${pi.partition} => $firstOffset:$lastOffset")
        })
        consumer.unsubscribe()
      }
    }


  }

  def createConsumerConfig(brokers: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GroupID)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }


}


private[commands] case class GetOffsetsActionConfig(brokerList: String = null,
                                                    topic: String = null)

object GetOffsetsAction extends CommandLineActionFactory {

  val Parser: OptionParser[GetOffsetsActionConfig] = new OptionParser[GetOffsetsActionConfig]("getOffsets") {
    opt[String]('b', "brokerList").required().action((s, c) =>
      c.copy(brokerList = s)).text("broker servers list")
    opt[String]('t', "topic").required().action((s, c) =>
      c.copy(topic = s)).text("broker servers list")
  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {

    Parser.parse(args, GetOffsetsActionConfig()) match {
      case Some(config) => Some(new GetOffsetsAction(config))
      case None => None
    }
  }

  override def renderUsage(mode: RenderingMode): String = Parser.renderUsage(mode)
}

