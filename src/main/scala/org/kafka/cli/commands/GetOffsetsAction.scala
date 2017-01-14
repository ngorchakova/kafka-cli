package org.kafka.cli.commands

import org.kafka.cli.{CommandLineActionFactory, CommandLineAction}
import scopt.OptionParser

/**
 * @author Natalia Gorchakova
 * @since  08.01.2017
 */
class GetOffsetsAction(val config: GetOffsetsActionConfig) extends CommandLineAction {
  override def perform() = {

  }
}

private[commands] case class GetOffsetsActionConfig(bootstrapServer: String = null,
                                                    topic: String = null)

object GetOffsetsAction extends CommandLineActionFactory {

  val Parser: OptionParser[GetOffsetsActionConfig] = new OptionParser[GetOffsetsActionConfig]("getOffsets") {
    opt[String]('b', "bootstrap").required().action((s, c) =>
      c.copy(bootstrapServer = s)).text("bootstrap service list")
    opt[String]('t', "topic").required().action((s, c) =>
      c.copy(topic = s)).text("bootstrap service list")
  }

  override def createAction(args: Seq[String]): Option[CommandLineAction] = {

    Parser.parse(args, new GetOffsetsActionConfig()) match {
      case Some(config) => Some(new GetOffsetsAction(config))
      case None => None
    }
  }
}
