package org.kafka.cli.commands

import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils
import org.kafka.cli.utils.KafkaOffsetTopicManager
import org.kafka.cli.{CommandLineAction, CommandLineActionFactory}
import scopt.{OptionParser, RenderingMode}

/**
  * @author menshin on 3/7/17.
  */
class DeleteConsumerGroupAction(val config: DeleteConsumerGroupActionConfig) extends CommandLineAction {

  override def perform(): Unit = {

    val zkUtils = ZkUtils(config.zookeeper,
      30000,
      30000,
      JaasUtils.isZkSecurityEnabled)

    val topicPartitions = zkUtils.getPartitionAssignmentForTopics(List(KafkaOffsetTopicManager.ConsumerOffsetTopic, config.topic))

    topicPartitions.get(KafkaOffsetTopicManager.ConsumerOffsetTopic) match {
      case Some(offsetPartitions) if offsetPartitions.nonEmpty =>
        val offsetsPartitionsCount = offsetPartitions.size
        println(s"__consumer_offsets topic has $offsetsPartitionsCount partitions")
        topicPartitions.get(config.topic) match {
          case Some(topicPartitionAssignment) if topicPartitionAssignment.nonEmpty =>

            val partitions = topicPartitionAssignment.keys
            val offsetKeys = partitions.map(partition => {
              KafkaOffsetTopicManager.offsetCommitKey(config.group, config.topic, partition)
            })
            /*val offsetTopicPartition = TopicAndPartition(GroupCoordinator.GroupMetadataTopicName, KafkaOffsetTopicManager.partitionFor(config.group, offsetsPartitionsCount))
            println(s"messages will go to $offsetTopicPartition")*/
            //TODO: send messages

            println("ready to send kafka key with null values to delete consumer records")
            offsetKeys.foreach(println)
          case _ =>
            println(s"Topic ${config.topic} doesn't exist!")
        }
      case _ => println(s"Failed to get partitions count for Topic ${KafkaOffsetTopicManager.ConsumerOffsetTopic}")
    }
  }
}


private[commands] case class DeleteConsumerGroupActionConfig(group: String = null, topic: String = null, zookeeper: String = null)

object DeleteConsumerGroupAction extends CommandLineActionFactory {

  val Parser: OptionParser[DeleteConsumerGroupActionConfig] = new OptionParser[DeleteConsumerGroupActionConfig]("deleteGroup") {
    opt[String]('t', "topic").required().action((t, c) =>
      c.copy(topic = t)).text("topic name")
    opt[String]('g', "group").required().action((s, c) =>
      c.copy(group = s)).text("group id")
    opt[String]('z', "zookeeper").required().action((z, c) =>
      c.copy(zookeeper = z)).text("zookeeper url")

  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {
    Parser.renderUsage(RenderingMode.TwoColumns)
    Parser.parse(args, DeleteConsumerGroupActionConfig()) match {
      case Some(config) => Some(new DeleteConsumerGroupAction(config))
      case None => None
    }
  }

  override def renderUsage(mode: RenderingMode): String = Parser.renderUsage(mode)
}
