package org.kafka.cli.commands

import java.util
import java.util.Properties
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.kafka.cli.{CommandLineAction, CommandLineActionFactory}
import org.kafka.cli.utils.TryWithClosable
import scopt.{OptionParser, RenderingMode}

import scala.collection.JavaConversions._

/**
  * @author menshin on 4/3/17.
  */
class SeekToBeginEndAction(val config: SeekToBeginEndActionConfig) extends CommandLineAction with TryWithClosable {

  override def perform(): Unit = {
    tryWith(new KafkaConsumer[String, String](createConsumerConfig(config.brokerList, config.groupId))) {
      consumer => {

        if (config.pattern) {
          consumer.subscribe(Pattern.compile(config.topic), new NoOpConsumerRebalanceListener())
        } else {
          consumer.subscribe(List(config.topic))
        }
        consumer.poll(100)
        if (config.toBegin) {
          println("seek to begin")
          consumer.seekToBeginning(consumer.assignment())
        } else {
          println("seek to end")
          consumer.seekToEnd(consumer.assignment())
        }
        consumer.poll(100)
        consumer.commitSync()

        consumer.assignment().toList.foreach { tp =>
          val currentPosition = consumer.position(tp)
          println(s"reset offset ${tp.topic}:${tp.partition} to $currentPosition")
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


private[commands] case class SeekToBeginEndActionConfig(brokerList: String = null,
                                                        topic: String = null,
                                                        pattern: Boolean = false,
                                                        groupId: String = null,
                                                        partitions: Set[Int] = Set.empty,
                                                        toBegin: Boolean = true)

object SeekToBeginEndAction extends CommandLineActionFactory {

  val Parser: OptionParser[SeekToBeginEndActionConfig] = new OptionParser[SeekToBeginEndActionConfig]("seekToBeginEnd") {
    opt[String]('b', "brokerList").required().action((s, c) =>
      c.copy(brokerList = s)).text("broker servers list")
    opt[String]('t', "topic").optional().action((s, c) =>
      c.copy(topic = s)).text("topic name")
    opt[Boolean]('m', "pattern").optional().action((s, c) =>
      c.copy(pattern = s)).text("topic field should be treated as pattern")
    opt[String]('g', "groupId").required().action((s, c) =>
      c.copy(groupId = s)).text("consumer group id")
    opt[String]('p', "partitions").action((s, c) =>
      c.copy(partitions = s.split(",").map(_.toInt).toSet)).text("comma separated list of partitions")
    opt[Boolean]('o', "toBegin").required().action((s, c) =>
      c.copy(toBegin = s)).text("true - from begin; false - to end")
  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {

    Parser.parse(args, SeekToBeginEndActionConfig()) match {
      case Some(config) => Some(new SeekToBeginEndAction(config))
      case None => None
    }
  }

  override def renderUsage(mode: RenderingMode): String = Parser.renderUsage(mode)
}