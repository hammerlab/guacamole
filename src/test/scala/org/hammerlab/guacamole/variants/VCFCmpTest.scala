package org.hammerlab.guacamole.variants

import java.nio.file.Files.lines
import java.nio.file.Path

import org.hammerlab.test.resources.File
import org.scalatest.Matchers

import scala.collection.JavaConverters._

trait VCFCmpTest {
  self: Matchers ⇒

  def vcfContentsIgnoringHeaders(path: Path): String =
    lines(path)
      .iterator()
      .asScala
      .filterNot(_.startsWith("##"))
      .mkString("\n")

  def checkVCFs(actualPath: Path, expectedPath: File): Unit = {
    vcfContentsIgnoringHeaders(actualPath) should be(vcfContentsIgnoringHeaders(expectedPath))
  }
}
