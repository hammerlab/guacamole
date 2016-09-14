package org.hammerlab.guacamole.variants

import org.scalatest.Matchers

trait VCFCmpTest extends Matchers {

  def vcfContentsIgnoringHeaders(path: String): String =
    scala.io.Source.fromFile(path).getLines().filterNot(_.startsWith("##")).mkString("\n")

  def checkVCFs(actualPath: String, expectedPath: String): Unit = {
    vcfContentsIgnoringHeaders(actualPath) should be(vcfContentsIgnoringHeaders(expectedPath))
  }
}
