package org.kafka.cli.commands

import kafka.admin.AdminUtils
import kafka.server.ConfigType
import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils
import org.kafka.cli.{CommandLineAction, CommandLineActionFactory}
import scopt.OptionParser
import java.util.Properties
import scala.collection.JavaConversions._

/**
  * @author Natalia Gorchakova
  * @since 08.01.2017
  */
class DescribeTopicAction(val config: DescribeTopicActionConfig) extends CommandLineAction {

  override def perform(): Unit = {
    val zkUtils = ZkUtils(config.zookeeper,
      30000,
      30000,
      JaasUtils.isZkSecurityEnabled)

    zkUtils.getPartitionAssignmentForTopics(List(config.topic)).get(config.topic) match {
      case Some(topicPartitionAssignment) =>
        val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
        val configs = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, config.topic)
        val liveBrokers = zkUtils.getAllBrokersInCluster().map(_.id).toSet

        if (configs.size() != 0) {
          val numPartitions = topicPartitionAssignment.size
          val replicationFactor = topicPartitionAssignment.head._2.size
          val topicConfiguration = configs.map(kv => kv._1 + "=" + kv._2).mkString(",")

          println(s"Topic: ${config.topic} \t" +
            s"PartitionCount: $numPartitions \t" +
            s"ReplicationFactor: $replicationFactor \t" +
            s"Configs:$topicConfiguration")
        }

        for ((partitionId, assignedReplicas) <- sortedPartitions) {
          val inSyncReplicas = zkUtils.getInSyncReplicasForPartition(config.topic, partitionId)
          val leader = zkUtils.getLeaderForPartition(config.topic, partitionId)
          print("\tTopic: " + config.topic)
          print("\tPartition: " + partitionId)
          print("\tLeader: " + (
            if (leader.isDefined) leader.get + (if (liveBrokers.contains(leader.get)) "" else " (inactive)")
            else "none"))
          print("\tReplicas: " + assignedReplicas.mkString(","))
          println("\tIsr: " + inSyncReplicas.mkString(","))

        }

      case None =>
        println(s"Topic ${config.topic} doesn't exist!")
    }

  }
}

private[commands] case class DescribeTopicActionConfig(zookeeper: String = null, topic: String = null)

object DescribeTopicAction extends CommandLineActionFactory {

  val Parser: OptionParser[DescribeTopicActionConfig] = new OptionParser[DescribeTopicActionConfig]("describeTopic") {
    opt[String]('t', "topic").required().action((t, c) =>
      c.copy(topic = t)).text("topic name")
    opt[String]('z', "zookeeper").required().action((s, c) =>
      c.copy(zookeeper = s)).text("zookeeper connect url")

  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {
    Parser.parse(args, DescribeTopicActionConfig()) match {
      case Some(config) => Some(new DescribeTopicAction(config))
      case None => None
    }
  }
}
