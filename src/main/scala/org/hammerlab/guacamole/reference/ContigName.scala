package org.hammerlab.guacamole.reference

/**
 * Helpers for dealing with contig-name strings.
 */
object ContigName {

  // Sort order: chr1, chr2, …, chr22, chrX, chrY, <other>.
  val ordering: Ordering[ContigName] =
    new Ordering[ContigName] {
      override def compare(x: ContigName, y: ContigName): Int =
        if (getContigRank(x) == notFound && getContigRank(y) == notFound)
          x.compare(y)
        else
          getContigRank(x) - getContigRank(y)
    }

  private def normalize(contig: ContigName): ContigName =
    if (contig.startsWith("chr"))
      contig.drop(3)
    else
      contig

  private def getContigRank(contig: ContigName): Int =
    map
      .getOrElse(
        normalize(contig),
        notFound
      )

  // Map from string contig name to ordered rank.
  private val map: Map[String, Int] = ((1 to 22).map(_.toString) ++ List("X", "Y")).zipWithIndex.toMap

  private val notFound = map.size
}
