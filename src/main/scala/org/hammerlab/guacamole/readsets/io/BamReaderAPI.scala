package org.hammerlab.guacamole.readsets.io

/**
 * Specification of which API to use when reading BAM and SAM files.
 *
 * Best - use Samtools when the file is on the local filesystem otherwise HadoopBAM
 * Samtools - use Samtools. This supports using the bam index when available.
 * HadoopBAM - use HadoopBAM. This supports reading the file in parallel when it is on HDFS.
 */
object BamReaderAPI extends Enumeration {
  type BamReaderAPI = Value
  val Best, Samtools, HadoopBAM = Value

  /**
   * Like Enumeration.withName but case insensitive
   */
  def withNameCaseInsensitive(s: String): Value = {
    val result = values.find(_.toString.compareToIgnoreCase(s) == 0)
    result.getOrElse(throw new IllegalArgumentException(
      "Unsupported bam reading API: %s. Valid APIs are: %s".format(s, values.map(_.toString).mkString(", "))))
  }
}
