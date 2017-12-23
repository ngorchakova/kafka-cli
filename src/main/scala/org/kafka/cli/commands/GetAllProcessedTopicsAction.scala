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
class GetAllProcessedTopicsAction (val config: GetAllProcessedTopicsActionConfig)  extends CommandLineAction with TryWithClosable{


  private val AdditionalGroupId = "GetAllProcessedTopicsAction"
  private val OffsetTopic = "__consumer_offsets"

  override def perform(): Unit = {

    // read __offsets topic
    tryWith(new KafkaConsumer[Array[Byte], Array[Byte]](createConsumerConfig(config.brokerList, AdditionalGroupId))) {
      consumer => {


        consumer.subscribe(List(OffsetTopic))
        consumer.poll(100)
        val partitions: Set[TopicPartition] = consumer.assignment().toSet
        println(s"consuming from $OffsetTopic topic and partitions ${partitions.mkString(", ")}")
        consumer.seekToEnd(partitions)

        // save till what partitions we would like to read __offsets topic
        val maxOffsets = collection.mutable.Map() ++ partitions.map(p => (p.partition(), consumer.position(p)))

        consumer.seekToBeginning(partitions)

        val topicOffset = mutable.Map[TopicPartition, Long]()

        while (maxOffsets.nonEmpty){
          val data = consumer.poll(10000)
          for (r <- data.iterator()){
            val key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(r.key()))
            key match {
              case offsetKey: OffsetKey =>
                val groupTopicPartition = offsetKey.key

                if (r.value() != null) {
                  val value = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(r.value))
                  if (config.groupId == groupTopicPartition.group){
                    //found our group
                    val committedOffset = value.offset
                    topicOffset.put(groupTopicPartition.topicPartition, committedOffset)

                    //println(s"found ${groupTopicPartition.topicPartition} with offset $committedOffset")
                  }
                }


              case _ => // no-op
            }

            //check that we reached end of some partitions
            maxOffsets.get(r.partition())
              .filter(o => o-1<=r.offset())
              .foreach{o => println(s"remove ${r.partition}"); maxOffsets.remove(r.partition())}

          }
        }
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