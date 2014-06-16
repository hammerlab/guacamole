package org.bdgenomics.guacamole

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.apache.spark.SparkContext

/**
 * A ReadSet contains an RDD of reads along with some metadata about them.
 *
 * @param reads RDD of reads
 * @param sequenceDictionary optional, sequence dictionary giving contigs and lengths
 * @param source string describing where these reads came from. Usually a filename.
 * @param filters filters used when reading these reads.
 * @param token token field for all of the reads in this set.
 * @param contigLengthsFromDictionary if true, the contigLengths property will use the sequence dictionary to get the
 *                                    contig lengths. Otherwise, the reads themselves will be used.
 */
case class ReadSet(
    reads: RDD[Read],
    sequenceDictionary: Option[SequenceDictionary],
    source: String,
    filters: Read.InputFilters,
    token: Int,
    contigLengthsFromDictionary: Boolean) {

  /** The reads, filtered to only mapped, and downcasted to MappedRead instances. */
  lazy val mappedReads = reads.filter(_.isMapped).map(_.getMappedRead())

  /**
   * A map from contig name -> length of contig.
   *
   * Can come either from the sequence dictionary, or by looking at the reads themselves, according to the
   * contigLengthsFromDictionary property.
   *
   */
  lazy val contigLengths: Map[String, Long] = {
    if (contigLengthsFromDictionary) {
      assume(sequenceDictionary.isDefined)
      val builder = Map.newBuilder[String, Long]
      sequenceDictionary.get.records.foreach(record => builder += ((record.name.toString, record.length)))
      builder.result
    } else {
      mappedReads.map(read => Map(read.referenceContig -> read.end)).reduce((map1, map2) => {
        val keys = map1.keySet.union(map2.keySet).toSeq
        keys.map(key => key -> math.max(map1.getOrElse(key, 0L), map2.getOrElse(key, 0L))).toMap
      }).toMap
    }
  }
}
object ReadSet {
  /**
   * Load reads from a file.
   *
   * @param sc spark context
   * @param filename bam or sam file to read from
   * @param filters input filters to filter reads while reading.
   * @param token token field for the reads
   * @param contigLengthsFromDictionary see [[ReadSet]] doc for description
   * @return
   */
  def apply(
    sc: SparkContext,
    filename: String,
    filters: Read.InputFilters = Read.InputFilters.empty,
    token: Int = 0,
    contigLengthsFromDictionary: Boolean = true): ReadSet = {

    val (reads, sequenceDictionary) = Read.loadReadRDDAndSequenceDictionaryFromBAM(
      filename,
      sc,
      token = token,
      filters = filters)
    new ReadSet(reads, Some(sequenceDictionary), filename, filters, token, contigLengthsFromDictionary)
  }
}
