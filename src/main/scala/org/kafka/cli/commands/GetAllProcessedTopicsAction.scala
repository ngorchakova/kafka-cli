package org.kafka.cli.commands

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Properties

import kafka.coordinator.{GroupMetadataManager, OffsetKey}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.kafka.cli.{CommandLineAction, CommandLineActionFactory}
import org.kafka.cli.utils.TryWithClosable
import scopt.{OptionParser, RenderingMode}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * @author menshin on 12/23/17.
  */
class GetAllProcessedTopicsAction(val config: GetAllProcessedTopicsActionConfig) extends CommandLineAction with TryWithClosable {


  private val AdditionalGroupId = "GetAllProcessedTopicsAction"
  private val OffsetTopic = "__consumer_offsets"

  override def perform(): Unit = {

    // read __offsets topic
    tryWith(new KafkaConsumer[Array[Byte], Array[Byte]](createConsumerConfig(config.brokerList, AdditionalGroupId))) {
      consumer => {
        val topicOffset = mutable.Set[String]()

        consumer.partitionsFor(OffsetTopic).foreach { pi =>
          println(s"processing $pi")
          val partition = new TopicPartition(pi.topic(), pi.partition())
          consumer.assign(List(partition))

          consumer.seekToEnd(List(partition))
          val maxOffset = consumer.position(partition) - 1

          consumer.seekToBeginning(List(partition))


          var finish = false
          while (!finish) {

            for (r <- consumer.poll(10000).iterator()) {
              val key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(r.key()))
              key match {
                case offsetKey: OffsetKey =>
                  val groupTopicPartition = offsetKey.key

                  if (r.value() != null) {
                    val value = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(r.value))
                    if (config.groupId == groupTopicPartition.group) {
                      //found our group
                      val committedOffset = value.offset
                      topicOffset.add(groupTopicPartition.topicPartition.topic())
                    }
                  }
                case _ => // no-op
              }

              if (maxOffset <= r.offset()) {
                finish = true
              }

            }
          }
        }

        topicOffset.foreach(println(_))

      }
    }
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props
  }
}

private[commands] case class GetAllProcessedTopicsActionConfig(brokerList: String = null,
                                                               groupId: String = null)

object GetAllProcessedTopicsAction extends CommandLineActionFactory {

  val Parser: OptionParser[GetAllProcessedTopicsActionConfig] = new OptionParser[GetAllProcessedTopicsActionConfig]("getAllProcessedTopics") {
    opt[String]('b', "brokerList").required().action((s, c) =>
      c.copy(brokerList = s)).text("broker servers list")
    opt[String]('g', "groupId").required().action((s, c) =>
      c.copy(groupId = s)).text("consumer group id")
  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {

    Parser.parse(args, GetAllProcessedTopicsActionConfig()) match {
      case Some(config) => Some(new GetAllProcessedTopicsAction(config))
      case None => None
    }
  }

  override def renderUsage(mode: RenderingMode): String = Parser.renderUsage(mode)
}