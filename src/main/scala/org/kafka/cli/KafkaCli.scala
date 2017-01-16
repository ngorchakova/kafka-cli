package org.kafka.cli

/**
  * @author Natalia Gorchakova
  * @since 03.01.2017
  */
object KafkaCli {


  def main(args: Array[String]): Unit = {
    var continue = true

    while (continue) {
      prepareLine()
      val line = readLine()
      continue = line != null && line != "exit"

      if (continue) {
        processAction(line)
      }
    }
  }

  def processAction(line: String) {
    line match {
      case CommandLineAction(a) => a.perform()
      case _ => println("unknown action")
    }
  }

  def prepareLine() = print("kafka-cli> ")

}
