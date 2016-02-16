package org.hammerlab.guacamole.commands.jointcaller

import org.apache.http.client.utils.URLEncodedUtils
import org.hammerlab.guacamole.commands.jointcaller.Input.{ Analyte, TissueType }

import scala.collection.JavaConversions

/**
 * An input BAM to the joint variant caller.
 *
 * The caller can work with any number of normal and tumor BAMs, each of which may be DNA or RNA.
 *
 * @param index Throughout the joint caller, we refer to inputs by index, which is the index (0 <= index < num inputs)
 *              of the input as specified on the commandline
 * @param name arbitrary name for this sample, used in VCF output
 * @param path path to BAM
 * @param tissueType tumor or normal
 * @param analyte rna or dna
 */
case class Input(index: Int, name: String, path: String, tissueType: TissueType.Value, analyte: Analyte.Value) {
  override def toString: String = {
    "<Input #%d '%s' of %s %s at %s >".format(index, name, tissueType, analyte, path)
  }

  // Some convenience properties.

  def normal = tissueType == TissueType.Normal
  def tumor = tissueType == TissueType.Tumor
  def dna = analyte == Analyte.DNA
  def rna = analyte == Analyte.RNA
  def normalDNA = normal && dna
  def tumorDNA = tumor && dna
  def normalRNA = normal && rna
  def tumorRNA = tumor && rna
}
object Input {
  /** Kind of tissue: tumor or normal. */
  object TissueType extends Enumeration {
    val Normal = Value("normal")
    val Tumor = Value("tumor")
  }

  /** Kind of sequencing: RNA or DNA. */
  object Analyte extends Enumeration {
    val DNA = Value("dna")
    val RNA = Value("rna")
  }

  /**
   * Given an index, a "url" (described below), and optionally some default properties, make an Input.
   *
   * The URL business is just a way of specifying the properties of an input on the commandline, using the 'fragment'
   * syntax available in URLs. For example these are all valid URLs:
   *
   *   /path/to/foo.bam
   *     Platin file; properties will be based on the 'defaults' argument
   *
   *   /path/to/foo.bam#analyte=rna
   *     Analyte specified as RNA; remaining properties come from 'default' argument
   *
   *   /path/to/foo.bam#analyte=rna&tissue_type=normal&name=my_important_sample
   *     Multiple properties can be specified, separate them with an '&' symbol.
   *
   * Someday we may want to use a more flexible commandline parser than args4j and get rid of the URLs.
   *
   * @param index index (0 <= index < num inputs) of this Input
   * @param url filename and optionally other properties specified after a '#' symbol, see above
   * @param defaults defaults for any properties not specified in the URL
   * @return Input instance
   */
  def apply(index: Int, url: String, defaults: Option[Input] = None): Input = {
    val parsed = new java.net.URI(url)
    val urlWithoutFragment = new java.net.URI(
      parsed.getScheme,
      parsed.getUserInfo,
      parsed.getHost,
      parsed.getPort,
      parsed.getPath,
      parsed.getQuery,
      "").toString.stripSuffix("#") // set fragment to the empty string

    val keyValues = URLEncodedUtils.parse(parsed.getFragment, org.apache.http.Consts.UTF_8)
    var tissueType: Option[TissueType.Value] = defaults.map(_.tissueType)
    var analyte: Option[Analyte.Value] = defaults.map(_.analyte)
    var name = defaults.map(_.name).filter(_.nonEmpty).getOrElse(
      urlWithoutFragment.split('/').last.stripSuffix(".bam"))
    JavaConversions.iterableAsScalaIterable(keyValues).foreach(pair => {
      val value = pair.getValue.toLowerCase
      pair.getName.toLowerCase match {
        case "tissue_type" => tissueType = Some(TissueType.withName(value))
        case "analyte"     => analyte = Some(Analyte.withName(value))
        case "name"        => name = value
        case other => {
          throw new IllegalArgumentException(
            "Unsupported input property: %s in %s. Valid properties are: tissue_type, analyte, name".format(
              other, url))
        }
      }
    })
    if (tissueType.isEmpty) {
      throw new IllegalArgumentException("No tissue_type specified for %s".format(url))
    }
    if (analyte.isEmpty) {
      throw new IllegalArgumentException("No analyte specified for %s".format(url))
    }
    new Input(index, name, urlWithoutFragment, tissueType.get, analyte.get)
  }
}
