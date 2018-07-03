package org.kafka.cli.commands

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.kafka.cli.utils.{ConsumerConfig, TryWithClosable}
import org.kafka.cli.{CommandLineAction, CommandLineActionFactory}
import scopt.{OptionParser, RenderingMode}

import scala.collection.JavaConversions._

/**
  * @author menshin on 5/4/17.
  */
class GetConsumerOffsetsAction(val config: GetConsumerOffsetsConfig) extends CommandLineAction with TryWithClosable {

  private val AdditionalGroupId = "GetConsumerOffsetsAction"

  override def perform(): Unit = {
    val consumerConfig = ConsumerConfig(config.brokerList, AdditionalGroupId, config.additionalConfig)

    tryWith(new KafkaConsumer[String, String](ConsumerConfig.createConsumerConfig(consumerConfig))) {
      consumer => {

        tryWith(new KafkaConsumer[String, String](ConsumerConfig.createConsumerConfig(consumerConfig.copy(groupId = AdditionalGroupId)))) {
          additionalConsumer => {

            consumer.subscribe(List(config.topic))
            additionalConsumer.subscribe(List(config.topic))
            consumer.poll(100)
            additionalConsumer.poll(100)
            additionalConsumer.seekToEnd(additionalConsumer.assignment())

            consumer.assignment().toList.foreach{tp =>
              val currentOffset = consumer.position(tp)
              val lastOffset = additionalConsumer.position(tp)
              val lag = lastOffset - currentOffset
              println(s"current offset ${tp.topic}:${tp.partition} $currentOffset / $lastOffset lag=$lag")
            }

          }
        }
      }
    }

  }


}


private[commands] case class GetConsumerOffsetsConfig(brokerList: String = null,
                                                      topic: String = null,
                                                      groupId: String = null,
                                                      additionalConfig: Option[String] = None)

object GetConsumerOffsetsAction extends CommandLineActionFactory {

  val Parser: OptionParser[GetConsumerOffsetsConfig] = new OptionParser[GetConsumerOffsetsConfig]("consumerOffsets") {
    opt[String]('b', "brokerList").required().action((s, c) =>
      c.copy(brokerList = s)).text("broker servers list")
    opt[String]('t', "topic").required().action((s, c) =>
      c.copy(topic = s)).text("topic name")
    opt[String]('g', "groupId").required().action((s, c) =>
      c.copy(groupId = s)).text("consumer group id")
    opt[String]('s', "securityConfig").optional().action((s, c) =>
      c.copy(additionalConfig = Some(s))).text("Config with additional security properties")
  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {

    Parser.parse(args, GetConsumerOffsetsConfig()) match {
      case Some(config) => Some(new GetConsumerOffsetsAction(config))
      case None => None
    }
  }

  override def renderUsage(mode: RenderingMode): String = Parser.renderUsage(mode)
}