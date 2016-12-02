package org.hammerlab.guacamole.util

object TestUtil {

  def resourcePath(filename: String): String = {
    val resource = Thread.currentThread().getContextClassLoader.getResource(filename)
    if (resource == null) throw new RuntimeException(s"No such test data file: $filename")
    resource.getFile
  }
}
