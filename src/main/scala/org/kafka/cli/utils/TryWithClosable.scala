package org.kafka.cli.utils

import java.io.Closeable

/**
  * @author menshin on 1/16/17.
  */
trait TryWithClosable {

  /**
    * Closes the resource after use.
    */
  def tryWith[A, R <: Closeable](r: R)(f: R => A): A =
    try {
      f(r)
    } finally {
      if (r != null) r.close()
    }
}
