package org.hammerlab.guacamole.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.hammerlab.guacamole.LociSet

/**
 * Filtering reads while they are loaded can be an important optimization.
 *
 * These fields are commonly used filters. For boolean fields, setting a field to true will result in filtering on
 * that field. The result is the intersection of the filters (i.e. reads must satisfy ALL filters).
 *
 * @param overlapsLoci if not None, include only mapped reads that overlap the given loci
 * @param nonDuplicate include only reads that do not have the duplicate bit set
 * @param passedVendorQualityChecks include only reads that do not have the failedVendorQualityChecks bit set
 * @param isPaired include only reads are paired-end reads
 */
case class ReadInputFilters(overlapsLoci: Option[LociSet.Builder],
                            nonDuplicate: Boolean,
                            passedVendorQualityChecks: Boolean,
                            isPaired: Boolean)

object ReadInputFilters {
  val empty = ReadInputFilters()

  /**
   * See ReadInputFilters for full documentation.
   *
   * @param mapped include only mapped reads. Convenience argument that is equivalent to specifying all sites in
   *               overlapsLoci.
   */
  def apply(mapped: Boolean = false,
            overlapsLoci: LociSet.Builder = null,
            nonDuplicate: Boolean = false,
            passedVendorQualityChecks: Boolean = false,
            isPaired: Boolean = false): ReadInputFilters = {
    new ReadInputFilters(
      overlapsLoci =
        if (overlapsLoci == null && mapped)
          Some(LociSet.newBuilder.putAllContigs)
        else
          Option(overlapsLoci),
      nonDuplicate,
      passedVendorQualityChecks,
      isPaired
    )
  }

  /**
   * Apply filters to an RDD of reads.
   *
   * @param filters
   * @param reads
   * @param sequenceDictionary
   * @return filtered RDD
   */
  def filterRDD(filters: ReadInputFilters, reads: RDD[Read], sequenceDictionary: SequenceDictionary): RDD[Read] = {
    /* Note that the InputFilter properties are public, and some loaders directly apply
     * the filters as the reads are loaded, instead of filtering an existing RDD as we do here. If more filters
     * are added, be sure to update those implementations.
     *
     * This is implemented as a static function instead of a method in ReadInputFilters because the overlapsLoci
     * attribute cannot be serialized.
     */
    var result = reads
    if (filters.overlapsLoci.nonEmpty) {
      val loci = filters.overlapsLoci.get.result(Read.contigLengths(sequenceDictionary))
      val broadcastLoci = reads.sparkContext.broadcast(loci)
      result = result.filter(
        read => read.isMapped && read.asMappedRead.get.overlapsLociSet(broadcastLoci.value))
    }
    if (filters.nonDuplicate) result = result.filter(!_.isDuplicate)
    if (filters.passedVendorQualityChecks) result = result.filter(!_.failedVendorQualityChecks)
    if (filters.isPaired) result = result.filter(_.isPaired)
    result
  }
}

