package org.kafka.cli

import org.kafka.cli.commands._
import scopt.RenderingMode

/**
 * @author Natalia Gorchakova
 * @since  08.01.2017
 */
trait CommandLineActionFactory {
  def renderUsage(mode: RenderingMode): String
  def createAction(args: Seq[String]): Option[CommandLineAction]
}

trait CommandLineAction {
  def perform()
}

object CommandLineAction {

  private val ActionMapping = Map(
    "describe" -> DescribeTopicAction,
    "topicList" -> ListTopicsAction,
    "getOffsets" -> GetOffsetsAction,
    "deleteGroup" -> DeleteConsumerGroupAction,
    "seekToBeginEnd" -> SeekToBeginEndAction,
    "consumerOffsets" -> GetConsumerOffsetsAction,
    "getMessage" -> GetMessageAction,
    "getAllProcessedTopics" -> GetAllProcessedTopicsAction
  )

  def printHelp(): Unit = {
    ActionMapping.values.map(_.renderUsage(RenderingMode.TwoColumns)).foreach(println)
  }

  def unapply(line: String): Option[CommandLineAction] = {
    val args = line.split(" ")


    args match {
      case Array(action, params@_*) if ActionMapping.contains(action) => ActionMapping(action).createAction(params)
      case _ => None
    }


  }
}
