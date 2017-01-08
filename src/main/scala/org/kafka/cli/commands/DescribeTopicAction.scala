package org.kafka.cli.commands

import org.kafka.cli.{CommandLineActionFactory, CommandLineAction}
import scopt.OptionParser

/**
 * @author Natalia Gorchakova
 * @since  08.01.2017
 */
class DescribeTopicAction(val config: DescribeTopicActionConfig) extends CommandLineAction {
  override def perform() = println(s"describe topic ${config.topic} for ${config.bootstrapServer}")
}

private[commands] case class DescribeTopicActionConfig(bootstrapServer: String = null, topic: String = null)

object DescribeTopicAction extends CommandLineActionFactory {

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {
    val parser: OptionParser[DescribeTopicActionConfig] = new OptionParser[DescribeTopicActionConfig]("describeTopic") {
      opt[String]('t', "topic").required().action((t, c) =>
        c.copy(topic = t)).text("topic name")
      opt[String]('b', "bootstrap").required().action((s, c) =>
        c.copy(bootstrapServer = s)).text("bootstrap service list")
    }
    parser.parse(args, DescribeTopicActionConfig()) match {
      case Some(config) => Some(new DescribeTopicAction(config))
      case None => None
    }
  }
}
