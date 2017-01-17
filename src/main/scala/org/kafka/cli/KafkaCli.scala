package org.kafka.cli

import jline.History
import org.jline.reader.LineReaderBuilder
import org.jline.terminal.TerminalBuilder

import scala.util.{Failure, Success, Try}

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
      .variable("history-file", ".history")
      .build()


    var continue = true

    while (continue) {
      Try(lineReader.readLine("kafka-cli> ")) match {
        case Success(line) =>
          continue = line != null && line != "exit"
          if (continue) {
            processAction(line)
          }

        case Failure(ex) => continue = false

      }
    }

    def processAction(line: String) {
      line match {
        case CommandLineAction(a) => a.perform()
        case _ => println("unknown action")
      }
    }

  }
}
