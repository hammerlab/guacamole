/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.reads

import java.io.File

import htsjdk.samtools._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.{ADAMContext, ADAMSpecificRecordSequenceDictionaryRDDAggregator}
import org.bdgenomics.formats.avro.AlignmentRecord
import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.seqdoop.hadoop_bam.util.SAMHeaderReader
import org.seqdoop.hadoop_bam.{AnySAMInputFormat, SAMRecordWritable}

import scala.collection.JavaConversions

/**
 * The fields in the Read trait are common to any read, whether mapped (aligned) or not.
 */
trait Read {

  /* The template name. A read, its mate, and any alternate alignments have the same name. */
  def name: String

  /** The nucleotide sequence. */
  def sequence: Seq[Byte]

  /** The base qualities, phred scaled.  These are numbers, and are NOT character encoded. */
  def baseQualities: Seq[Byte]

  /** Is this read a duplicate of another? */
  def isDuplicate: Boolean

  /** Is this read mapped? */
  def isMapped: Boolean

  def asMappedRead: Option[MappedRead]

  /** The sample (e.g. "tumor" or "patient3636") name. */
  def sampleName: String

  /** Whether the read failed predefined vendor checks for quality */
  def failedVendorQualityChecks: Boolean

  /** Whether read is from a paired-end library */
  def isPaired: Boolean

}

object Read extends Logging {
  /**
   * Converts the ascii-string encoded base qualities into an array of integers
   * quality scores in Phred-scale
   *
   * @param baseQualities Base qualities of a read (ascii-encoded)
   * @param length Length of the read sequence
   * @return  Base qualities in Phred scale
   */
  def baseQualityStringToArray(baseQualities: String, length: Int): Array[Byte] = {

    // If no base qualities are set, we set them all to 0.
    if (baseQualities.isEmpty)
      (0 until length).map(_ => 0.toByte).toArray
    else
      baseQualities.map(q => (q - 33).toByte).toArray

  }

  /**
   * Convert a SAM tools record into a Read.
   *
   * @param record
   * @return
   */
  def fromSAMRecord(record: SAMRecord): Read = {

    val isMapped = (
      !record.getReadUnmappedFlag &&
      record.getReferenceName != null &&
      record.getReferenceIndex >= SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX &&
      record.getAlignmentStart >= 0 &&
      record.getUnclippedStart >= 0)

    val sampleName = (if (record.getReadGroup != null && record.getReadGroup.getSample != null) {
      record.getReadGroup.getSample
    } else {
      "default"
    }).intern

    val read = if (isMapped) {
      val result = MappedRead(
        record.getReadName,
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName.intern,
        record.getReferenceName.intern,
        record.getMappingQuality,
        record.getAlignmentStart - 1,
        cigar = record.getCigar,
        failedVendorQualityChecks = record.getReadFailsVendorQualityCheckFlag,
        isPositiveStrand = !record.getReadNegativeStrandFlag,
        isPaired = record.getReadPairedFlag
      )

      // We subtract 1 from start, since samtools is 1-based and we're 0-based.
      if (result.unclippedStart != record.getUnclippedStart - 1)
        log.warn("Computed read 'unclippedStart' %d != samtools read end %d.".format(
          result.unclippedStart, record.getUnclippedStart - 1))
      result
    } else {
      UnmappedRead(
        record.getReadName,
        record.getReadString.getBytes,
        record.getBaseQualities,
        record.getDuplicateReadFlag,
        sampleName,
        record.getReadFailsVendorQualityCheckFlag,
        record.getReadPairedFlag
      )
    }
    if (record.getReadPairedFlag) {
      val mateAlignment = MateAlignmentProperties(record)
      PairedRead(read, isFirstInPair = record.getFirstOfPairFlag, mateAlignment)
    } else {
      read
    }
  }

  /**
   * Given a filename and a spark context, return a pair (RDD, SequenceDictionary), where the first element is an RDD
   * of Reads, and the second element is the Sequence Dictionary giving info (e.g. length) about the contigs in the BAM.
   *
   * @param filename name of file containing reads
   * @param sc spark context
   * @param filters filters to apply
   * @return
   */
  def loadReadRDDAndSequenceDictionary(filename: String,
                                       sc: SparkContext,
                                       filters: ReadInputFilters,
                                       config: ReadLoadingConfig = ReadLoadingConfig.default): (RDD[Read], SequenceDictionary) = {
    if (filename.endsWith(".bam") || filename.endsWith(".sam")) {
      loadReadRDDAndSequenceDictionaryFromBAM(
        filename,
        sc,
        filters,
        config
      )
    } else {
      loadReadRDDAndSequenceDictionaryFromADAM(
        filename,
        sc,
        filters,
        config
      )
    }
  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in BAM format **/
  def loadReadRDDAndSequenceDictionaryFromBAM(
    filename: String,
    sc: SparkContext,
    filters: ReadInputFilters = ReadInputFilters.empty,
    config: ReadLoadingConfig = ReadLoadingConfig.default): (RDD[Read], SequenceDictionary) = {

    val path = new Path(filename)
    val scheme = path.getFileSystem(sc.hadoopConfiguration).getScheme
    val useSamtools =
      config.bamReaderAPI == BamReaderAPI.Samtools ||
        (
          config.bamReaderAPI == BamReaderAPI.Best &&
          scheme == "file"
        )

    if (useSamtools) {
      // Load with samtools
      if (scheme != "file") {
        throw new IllegalArgumentException(
          "Samtools API can only be used to read local files (i.e. scheme='file'), not: scheme='%s'".format(scheme))
      }
      var requiresFilteringByLocus = filters.overlapsLoci.nonEmpty

      SamReaderFactory.setDefaultValidationStringency(ValidationStringency.SILENT)
      val factory = SamReaderFactory.makeDefault
      val reader = factory.open(new File(path.toUri.getPath))
      val samSequenceDictionary = reader.getFileHeader.getSequenceDictionary
      val sequenceDictionary = SequenceDictionary.fromSAMSequenceDictionary(samSequenceDictionary)
      val loci = filters.overlapsLoci.map(_.result(contigLengths(sequenceDictionary)))
      val recordIterator: SAMRecordIterator = if (filters.overlapsLoci.nonEmpty && reader.hasIndex) {
        progress(s"Using samtools with BAM index to read: $filename")
        requiresFilteringByLocus = false
        val queryIntervals = loci.get.contigs.flatMap(contig => {
          val contigIndex = samSequenceDictionary.getSequenceIndex(contig)
          loci.get.onContig(contig).ranges().map(range =>
            new QueryInterval(contigIndex,
              range.start.toInt + 1, // convert 0-indexed inclusive to 1-indexed inclusive
              range.end.toInt)) // "convert" 0-indexed exclusive to 1-indexed inclusive, which is a no-op)
        })
        val optimizedQueryIntervals = QueryInterval.optimizeIntervals(queryIntervals.toArray)
        reader.query(optimizedQueryIntervals, false) // Note: this can return unmapped reads, which we filter below.
      } else {

        val skippedReason =
          if (reader.hasIndex)
            "index is available but not needed"
          else
            "index unavailable"

        progress(s"Using samtools without BAM index %($skippedReason) to read: $filename")

        reader.iterator
      }
      val reads = JavaConversions.asScalaIterator(recordIterator).flatMap(record => {
        // Optimization: some of the filters are easy to run on the raw SamRecord, so we avoid making a Read.
        if ((filters.overlapsLoci.nonEmpty && record.getReadUnmappedFlag) ||
          (requiresFilteringByLocus &&
            !loci.get.onContig(record.getContig).intersects(record.getStart - 1, record.getEnd)) ||
            (filters.nonDuplicate && record.getDuplicateReadFlag) ||
            (filters.passedVendorQualityChecks && record.getReadFailsVendorQualityCheckFlag) ||
            (filters.isPaired && !record.getReadPairedFlag)) {
          None
        } else {
          val read = fromSAMRecord(record)
          assert(filters.overlapsLoci.isEmpty || read.isMapped)
          Some(read)
        }
      })
      (sc.parallelize(reads.toSeq), sequenceDictionary)
    } else {
      // Load with hadoop bam
      progress(s"Using hadoop bam to read: $filename")
      val samHeader = SAMHeaderReader.readSAMHeaderFrom(path, sc.hadoopConfiguration)
      val sequenceDictionary = SequenceDictionary.fromSAMHeader(samHeader)

      val samRecords: RDD[(LongWritable, SAMRecordWritable)] =
        sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](filename)
      val allReads: RDD[Read] =
        samRecords.map {
          case (k, v) => fromSAMRecord(v.get)
        }
      val reads = ReadInputFilters.filterRDD(filters, allReads, sequenceDictionary)
      (reads, sequenceDictionary)
    }
  }

  /** Returns an RDD of Reads and SequenceDictionary from reads in ADAM format **/
  def loadReadRDDAndSequenceDictionaryFromADAM(filename: String,
                                               sc: SparkContext,
                                               filters: ReadInputFilters = ReadInputFilters.empty,
                                               config: ReadLoadingConfig = ReadLoadingConfig.default): (RDD[Read], SequenceDictionary) = {

    progress(s"Using ADAM to read: $filename")

    val adamContext = new ADAMContext(sc)

    val adamRecords: RDD[AlignmentRecord] = adamContext.loadAlignments(
      filename, projection = None, stringency = ValidationStringency.LENIENT).rdd
    val sequenceDictionary = new ADAMSpecificRecordSequenceDictionaryRDDAggregator(adamRecords).adamGetSequenceDictionary()

    val allReads: RDD[Read] = adamRecords.map(fromADAMRecord(_))
    val reads = ReadInputFilters.filterRDD(filters, allReads, sequenceDictionary)
    (reads, sequenceDictionary)
  }

  /**
   *
   * Builds a Guacamole Read from and ADAM Alignment Record
   *
   * @param alignmentRecord ADAM Alignment Record (an aligned or unaligned read)
   * @return Mapped or Unmapped Read
   */
  def fromADAMRecord(alignmentRecord: AlignmentRecord): Read = {

    val sequence = Bases.stringToBases(alignmentRecord.getSequence)
    val baseQualities = baseQualityStringToArray(alignmentRecord.getQual, sequence.length)

    val referenceContig = alignmentRecord.getContig.getContigName.intern
    val cigar = TextCigarCodec.decode(alignmentRecord.getCigar)

    val read = if (alignmentRecord.getReadMapped) {
      MappedRead(
        name = alignmentRecord.getReadName,
        sequence = sequence,
        baseQualities = baseQualities,
        isDuplicate = alignmentRecord.getDuplicateRead,
        sampleName = alignmentRecord.getRecordGroupSample.intern(),
        referenceContig = referenceContig,
        alignmentQuality = alignmentRecord.getMapq,
        start = alignmentRecord.getStart,
        cigar = cigar,
        failedVendorQualityChecks = alignmentRecord.getFailedVendorQualityChecks,
        isPositiveStrand = !alignmentRecord.getReadNegativeStrand,
        alignmentRecord.getReadPaired
      )
    } else {
      UnmappedRead(
        name = alignmentRecord.getReadName,
        sequence = sequence,
        baseQualities = baseQualities,
        isDuplicate = alignmentRecord.getDuplicateRead,
        sampleName = alignmentRecord.getRecordGroupSample.intern(),
        failedVendorQualityChecks = alignmentRecord.getFailedVendorQualityChecks,
        alignmentRecord.getReadPaired
      )
    }

    if (alignmentRecord.getReadPaired) {
      val mateAlignment = if (alignmentRecord.getMateMapped) Some(
        MateAlignmentProperties(
          referenceContig = alignmentRecord.getMateContig.getContigName.intern(),
          start = alignmentRecord.getMateAlignmentStart,
          inferredInsertSize = if (alignmentRecord.getInferredInsertSize != 0 && alignmentRecord.getInferredInsertSize != null) Some(alignmentRecord.getInferredInsertSize.toInt) else None,
          isPositiveStrand = !alignmentRecord.getMateNegativeStrand
        )
      )
      else
        None
      PairedRead(read, isFirstInPair = alignmentRecord.getReadInFragment == 1, mateAlignment)
    } else {
      read
    }
  }

  /** Extract the length of each contig from a sequence dictionary */
  def contigLengths(sequenceDictionary: SequenceDictionary): Map[String, Long] = {
    val builder = Map.newBuilder[String, Long]
    sequenceDictionary.records.foreach(record => builder += ((record.name.toString, record.length)))
    builder.result
  }

}
