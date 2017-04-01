package org.hammerlab.guacamole.variants

import org.hammerlab.paths.Path
import org.hammerlab.test.resources.File
import org.scalatest.Matchers

trait VCFCmpTest {
  self: Matchers ⇒

  def vcfContentsIgnoringHeaders(path: Path): String =
    path
      .lines
      .filterNot(_.startsWith("##"))
      .mkString("\n")

  def checkVCFs(actualPath: Path, expectedPath: File): Unit = {
    vcfContentsIgnoringHeaders(actualPath) should be(vcfContentsIgnoringHeaders(expectedPath))
  }
}
