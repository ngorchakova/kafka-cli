package org.kafka.cli.utils

import java.io.FileInputStream
import java.util.Properties


/**
  * @author menshin on 7/2/18.
  */
object PropertyUtils extends TryWithClosable{

  def loadProps(path: String) : Properties ={
    val properties = new Properties()
    tryWith(new FileInputStream(path)){ fis =>
      properties.load(fis)
    }

    properties
  }

}
