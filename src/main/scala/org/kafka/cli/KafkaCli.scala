package org.kafka.cli

import org.jline.reader.LineReaderBuilder
import org.jline.terminal.TerminalBuilder

/**
  * @author Natalia Gorchakova
  * @since 03.01.2017
  */
object KafkaCli {


  def main(args: Array[String]): Unit = {
    val terminal = TerminalBuilder.builder()
      .name("kafka-cli")
      .system(true)
      .build()

    val lineReader = LineReaderBuilder.builder()
      .terminal(terminal)
      .build()


    var continue = true

    while (continue) {
      val line = lineReader.readLine("kafka-cli> ")
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

}
