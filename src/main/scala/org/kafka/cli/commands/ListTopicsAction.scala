package org.kafka.cli.commands

import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils
import org.kafka.cli.{CommandLineAction, CommandLineActionFactory}
import scopt.OptionParser

/**
  * @author Natalia Gorchakova
  * @since 08.01.2017
  */
class ListTopicsAction(val config: ListTopicsActionConfig) extends CommandLineAction {
  override def perform(): Unit = {
    val zkUtils = ZkUtils(config.zookeeper,
      30000,
      30000,
      JaasUtils.isZkSecurityEnabled)

    val topics = zkUtils.getAllTopics()
    val displayedTopics = topics
      .filter(topic => {
        config.topicNamePart.isEmpty || topic.contains(config.topicNamePart.get)
      })
    displayedTopics
      .sorted
      .foreach(println)
    println(s"total count: ${displayedTopics.size}")
  }
}

private[commands] case class ListTopicsActionConfig(zookeeper: String = null, topicNamePart: Option[String] = None)

object ListTopicsAction extends CommandLineActionFactory {

  val Parser: OptionParser[ListTopicsActionConfig] = new OptionParser[ListTopicsActionConfig]("list all topics") {
    opt[String]('z', "zookeeper").required().action((s, c) =>
      c.copy(zookeeper = s)).text("zookeeper connect url")

    opt[String]('p', "topicNamePart").action((s, c) =>
      c.copy(topicNamePart = Some(s))).text("part of topic name")
  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {

    Parser.parse(args, ListTopicsActionConfig()) match {
      case Some(config) => Some(new ListTopicsAction(config))
      case None => None
    }
  }
}