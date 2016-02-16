package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.commands.jointcaller.Input.{Analyte, TissueType}

/**
 * Convenience container for zero or more Input instances.
 */
case class InputCollection(items: Seq[Input]) {
  val normalDNA = items.filter(_.normalDNA)
  val normalRNA = items.filter(_.normalRNA)
  val tumorDNA = items.filter(_.tumorDNA)
  val tumorRNA = items.filter(_.tumorRNA)
}

object InputCollection {
  /**
   * Given a sequence of URLs (see the docs for `Input.apply`), return an InputCollection.
   *
   * By default, the first input is taken to be a normal DNA sample, and the subsequent inputs are tumor DNA samples.
   * This can be changed by specifying the tissue_type and analyte of the samples in the URL.
   */
  def parseMultiple(urls: Seq[String]): InputCollection = {
    var default = Input(-1, "", "", TissueType.Normal, Analyte.DNA)
    val items = urls.zipWithIndex.map({
      case (url, index) => {
        val result = Input(index, url, Some(default))
        default = Input(-1, "", "", TissueType.Tumor, Analyte.DNA)
        result
      }
    })
    InputCollection(items)
  }
}
