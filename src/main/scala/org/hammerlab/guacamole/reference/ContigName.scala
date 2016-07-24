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

  def normalize(contig: ContigName): ContigName =
    if (contig.startsWith("chr"))
      contig.drop(3)
    else
      contig

  def getContigRank(contig: ContigName): Int = ContigName.map.getOrElse(normalize(contig), notFound)

  val map: Map[String, Int] =
    Map(
       "1" ->  1,
       "2" ->  2,
       "3" ->  3,
       "4" ->  4,
       "5" ->  5,
       "6" ->  6,
       "7" ->  7,
       "8" ->  8,
       "9" ->  9,
      "10" -> 10,
      "11" -> 11,
      "12" -> 12,
      "13" -> 13,
      "14" -> 14,
      "15" -> 15,
      "16" -> 16,
      "17" -> 17,
      "18" -> 18,
      "19" -> 19,
      "20" -> 20,
      "21" -> 21,
      "22" -> 22,
       "X" -> 23,
       "Y" -> 24
    )

  val notFound = map.size + 1
}
