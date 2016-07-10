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

package org.hammerlab.guacamole.util

import java.io.{File, FileNotFoundException}
import java.nio.file.Files

import htsjdk.samtools.TextCigarCodec
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, MateAlignmentProperties, PairedRead, Read}
import org.hammerlab.guacamole.readsets.{InputFilters, ReadLoadingConfig, ReadSets, ReadsArgs, ReadsRDD}
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.reference.{ContigSequence, ReferenceBroadcast}

import scala.collection.mutable
import scala.math._

object TestUtil {

  object Implicits {
    implicit def basesToString = Bases.basesToString _
    implicit def stringToBases = Bases.stringToBases _
  }

  def tmpPath(suffix: String): String = {
    new File(Files.createTempDirectory("TestUtil").toFile, s"TestUtil$suffix").getAbsolutePath
  }

  /**
   * Make a ReferenceBroadcast containing the specified sequences to be used in tests.
   *
   * @param sc
   * @param contigStartSequences tuples of (contig name, start, reference sequence) giving the desired sequences
   * @param contigLengths total length of each contigs (for simplicity all contigs are assumed to have the same length)
   * @return a map acked ReferenceBroadcast containing the desired sequences
   */
  def makeReference(sc: SparkContext,
                    contigStartSequences: Seq[(String, Int, String)],
                    contigLengths: Int = 1000): ReferenceBroadcast = {
    val map = mutable.HashMap[String, ContigSequence]()
    contigStartSequences.foreach({
      case (contig, start, sequence) => {
        val locusToBase = Bases.stringToBases(sequence).zipWithIndex.map(pair => (pair._2 + start, pair._1)).toMap
        map.put(contig, MapBackedReferenceSequence(contigLengths, sc.broadcast(locusToBase)))
      }
    })
    new ReferenceBroadcast(map.toMap, source=Some("test_values"))
  }

  /**
   * Convenience function to construct a Read from unparsed values.
   */
  private def read(sequence: String,
                   name: String,
                   baseQualities: String = "",
                   isDuplicate: Boolean = false,
                   sampleName: String = "",
                   referenceContig: String = "",
                   alignmentQuality: Int = -1,
                   start: Long = -1L,
                   cigarString: String = "",
                   failedVendorQualityChecks: Boolean = false,
                   isPositiveStrand: Boolean = true,
                   isPaired: Boolean = true) = {

    val sequenceArray = sequence.map(_.toByte).toArray
    val qualityScoresArray = Read.baseQualityStringToArray(baseQualities, sequenceArray.length)

    val cigar = TextCigarCodec.decode(cigarString)
    MappedRead(
      name,
      sequenceArray,
      qualityScoresArray,
      isDuplicate,
      sampleName.intern,
      referenceContig,
      alignmentQuality,
      start,
      cigar,
      failedVendorQualityChecks,
      isPositiveStrand,
      isPaired
    )
  }

  def makeRead(sequence: String,
               cigar: String = "",
               start: Long = 1,
               chr: String = "chr1",
               qualityScores: Option[Seq[Int]] = None,
               alignmentQuality: Int = 30): MappedRead = {

    val qualityScoreString = if (qualityScores.isDefined) {
      qualityScores.get.map(q => q + 33).map(_.toChar).mkString
    } else {
      sequence.map(x => '@').mkString
    }

    read(
      sequence,
      name = "read1",
      cigarString = cigar,
      start = start,
      referenceContig = chr,
      baseQualities = qualityScoreString,
      alignmentQuality = alignmentQuality
    )

  }

  def makePairedRead(
    chr: String = "chr1",
    start: Long = 1,
    alignmentQuality: Int = 30,
    isPositiveStrand: Boolean = true,
    isMateMapped: Boolean = false,
    mateReferenceContig: Option[String] = None,
    mateStart: Option[Long] = None,
    isMatePositiveStrand: Boolean = false,
    sequence: String = "ACTGACTGACTG",
    cigar: String = "12M",
    inferredInsertSize: Option[Int]): PairedRead[MappedRead] = {

    val qualityScoreString = sequence.map(x => '@').mkString

    PairedRead(
      read(
        sequence,
        name = "read1",
        cigarString = cigar,
        start = start,
        referenceContig = chr,
        isPositiveStrand = isPositiveStrand,
        baseQualities = qualityScoreString,
        alignmentQuality = alignmentQuality,
        isPaired = true
      ),
      isFirstInPair = true,
      mateAlignmentProperties =
        if (isMateMapped)
          Some(
            MateAlignmentProperties(
              mateReferenceContig.get,
              mateStart.get,
              inferredInsertSize = inferredInsertSize,
              isPositiveStrand = isMatePositiveStrand
            )
          )
        else
          None
    )
  }

  def testDataPath(filename: String): String = {
    // If we have an absolute path, just return it.
    if (new File(filename).isAbsolute) {
      filename
    } else {
      val resource = ClassLoader.getSystemClassLoader.getResource(filename)
      if (resource == null) throw new RuntimeException("No such test data file: %s".format(filename))
      resource.getFile
    }
  }

  def loadTumorNormalReads(sc: SparkContext,
                           tumorFile: String,
                           normalFile: String): (Seq[MappedRead], Seq[MappedRead]) = {
    val filters = InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    (
      loadReads(sc, tumorFile, filters = filters).mappedReads.collect(),
      loadReads(sc, normalFile, filters = filters).mappedReads.collect()
    )
  }

  def loadReads(sc: SparkContext,
                filename: String,
                filters: InputFilters = InputFilters.empty,
                config: ReadLoadingConfig = ReadLoadingConfig.default): ReadsRDD = {
    // Grab the path to the SAM file in the resources subdirectory.
    val path = testDataPath(filename)
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    val args = new ReadsArgs {}
    args.reads = path
    ReadSets.loadReads(args, sc, filters)._1
  }

  def loadTumorNormalPileup(tumorReads: Seq[MappedRead],
                            normalReads: Seq[MappedRead],
                            locus: Long,
                            reference: ReferenceBroadcast): (Pileup, Pileup) = {
    val contig = tumorReads(0).contig
    assume(normalReads(0).contig == contig)
    (Pileup(tumorReads, contig, locus, reference.getContig(contig)),
      Pileup(normalReads, contig, locus, reference.getContig(contig)))
  }

  def loadPileup(sc: SparkContext,
                 filename: String,
                 reference: ReferenceBroadcast,
                 locus: Long = 0,
                 maybeContig: Option[String] = None): Pileup = {
    val records =
      TestUtil.loadReads(
        sc,
        filename,
        filters = InputFilters(
          overlapsLoci = maybeContig.map(
            contig => LociParser(s"$contig:$locus-${locus + 1}")
          ).orNull
        )
      ).mappedReads
    val localReads = records.collect
    val actualContig = maybeContig.getOrElse(localReads(0).contig)
    Pileup(
      localReads,
      actualContig,
      locus,
      reference = reference.getContig(actualContig)
    )
  }

  def assertAlmostEqual(a: Double, b: Double, epsilon: Double = 1e-12) {
    assert(abs(a - b) < epsilon, "|%.12f - %.12f| == %.12f >= %.12f".format(a, b, abs(a - b), epsilon))
  }

  /**
   * Delete a file or directory (recursively) if it exists.
   */
  def deleteIfExists(filename: String) = {
    val file = new File(filename)
    try {
      FileUtils.forceDelete(file)
    } catch {
      case e: FileNotFoundException => {}
    }
  }

}
