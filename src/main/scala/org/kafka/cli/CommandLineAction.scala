package org.kafka.cli

import org.kafka.cli.commands.{GetOffsetsAction, ListTopicsAction, DescribeTopicAction}

/**
 * @author Natalia Gorchakova
 * @since  08.01.2017
 */
trait CommandLineActionFactory {
  def createAction(args: Seq[String]): Option[CommandLineAction]
}

trait CommandLineAction {
  def perform()
}

object CommandLineAction {

  private val ActionMapping = Map(
    "describe" -> DescribeTopicAction,
    "topicList" -> ListTopicsAction,
    "getOffsets" -> GetOffsetsAction
  )

  def unapply(line: String): Option[CommandLineAction] = {
    val args = line.split(" ")


    args match {
      case Array(action, params@_*) if ActionMapping.contains(action) => ActionMapping(action).createAction(params)
      case _ => None
    }


  }
}
