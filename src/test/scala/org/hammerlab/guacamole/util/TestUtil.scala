package org.hammerlab.guacamole.util

import java.io.File
import java.nio.file.Files

object TestUtil {

  object Implicits {
    implicit def basesToString = Bases.basesToString _
    implicit def stringToBases = Bases.stringToBases _
  }

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
