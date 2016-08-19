package org.hammerlab.guacamole.util

import java.io.File
import java.nio.file.Files

object TestUtil {

  def tmpPath(suffix: String): String = {
    new File(Files.createTempDirectory("TestUtil").toFile, s"TestUtil$suffix").getAbsolutePath
  }

  def testDataPath(filename: String): String = {
    // If we have an absolute path, just return it.
    if (new File(filename).isAbsolute) {
      filename
    } else {
      val resource = ClassLoader.getSystemClassLoader.getResource(filename)
      if (resource == null) throw new RuntimeException("No such test data file: %s".format(filename))
      resource.getFile
    }
  }
}
