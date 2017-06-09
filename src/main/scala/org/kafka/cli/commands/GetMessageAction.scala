package org.kafka.cli.commands

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.kafka.cli.{CommandLineAction, CommandLineActionFactory}
import org.kafka.cli.utils.TryWithClosable
import scopt.{OptionParser, RenderingMode}
import scala.collection.JavaConversions._


/**
  * @author menshin on 6/9/17.
  */
class GetMessageAction(val config: GetMessageConfig) extends CommandLineAction with TryWithClosable {

  private val AdditionalGroupId = "GetMessageAction"

  override def perform(): Unit = {
    tryWith(new KafkaConsumer[String, String](createConsumerConfig(config.brokerList,AdditionalGroupId))) {
      consumer => {
        val topicPartition = new TopicPartition(config.topic, config.partition)

        consumer.assign(List(topicPartition))
        consumer.seek(topicPartition, config.offset)


        val messages = consumer.poll(10000)
        messages.headOption.foreach(r => {
          println(s"key: ${r.key}")
          println(s"${r.value()}")
        })

        consumer.unsubscribe()


      }
    }

  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }


}


private[commands] case class GetMessageConfig(brokerList: String = null,
                                              topic: String = null,
                                              partition: Integer = 0,
                                              offset: Long = -1L)

object GetMessageAction extends CommandLineActionFactory {

  val Parser: OptionParser[GetMessageConfig] = new OptionParser[GetMessageConfig]("consumerOffsets") {
    opt[String]('b', "brokerList").required().action((s, c) =>
      c.copy(brokerList = s)).text("broker servers list")
    opt[String]('t', "topic").required().action((s, c) =>
      c.copy(topic = s)).text("topic name")
    opt[Long]('p', "partition").required().action((s, c) =>
      c.copy(partition = s.toInt)).text("partition")
    opt[Long]('o', "offset").required().action((s, c) =>
      c.copy(offset = s)).text("partition")
  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {

    Parser.parse(args, GetMessageConfig()) match {
      case Some(config) => Some(new GetMessageAction(config))
      case None => None
    }
  }

  override def renderUsage(mode: RenderingMode): String = Parser.renderUsage(mode)
}