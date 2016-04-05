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
import java.util.UUID

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.{IKryoRegistrar, KryoInstantiator, KryoPool}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.Read.InputFilters
import org.hammerlab.guacamole.reads._
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.reference.{ContigSequence, ReferenceBroadcast}
import org.hammerlab.guacamole.{Bases, GuacamoleKryoRegistrator, LociSet, ReadSet}
import org.scalatest._

import scala.collection.mutable
import scala.math._

object TestUtil extends Matchers {

  object Implicits {
    implicit def basesToString = Bases.basesToString _
    implicit def stringToBases = Bases.stringToBases _
  }

  def tmpFileName(suffix: String): String = {
    "TestUtil." + UUID.randomUUID() + suffix
  }

  // Serialization helper functions.
  lazy val kryoPool = {
    val instantiator = new KryoInstantiator().setRegistrationRequired(true).withRegistrar(new IKryoRegistrar {
      override def apply(kryo: Kryo): Unit = new GuacamoleKryoRegistrator().registerClasses(kryo)
    })
    KryoPool.withByteArrayOutputStream(1, instantiator)
  }
  def serialize(item: Any): Array[Byte] = {
    kryoPool.toBytesWithClass(item)
  }
  def deserialize[T](bytes: Array[Byte]): T = {
    kryoPool.fromBytes(bytes).asInstanceOf[T]
  }
  def testSerialization[T](item: T): Unit = {
    val serialized = serialize(item)
    val deserialized = deserialize[T](serialized)
    deserialized should equal(item)
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
    new ReferenceBroadcast(map.toMap)
  }

  def makeRead(sequence: String,
               cigar: String,
               start: Long = 1,
               chr: String = "chr1",
               qualityScores: Option[Seq[Int]] = None,
               alignmentQuality: Int = 30): MappedRead = {

    val qualityScoreString = if (qualityScores.isDefined) {
      qualityScores.get.map(q => q + 33).map(_.toChar).mkString
    } else {
      sequence.map(x => '@').mkString
    }

    Read(
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
      Read(
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
        if (isMateMapped) Some(
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

  def makePairedMappedRead(
    chr: String = "chr1",
    start: Long = 1,
    alignmentQuality: Int = 30,
    isPositiveStrand: Boolean = true,
    mateReferenceContig: String = "chr1",
    mateStart: Long = 1,
    isMatePositiveStrand: Boolean = false,
    inferredInsertSize: Option[Int] = None,
    sequence: String = "ACTGACTGACTG",
    cigar: String = "12M",
    mdTag: String = "12"): PairedMappedRead = {

    val insertSize = inferredInsertSize.getOrElse({
      val minStart = Math.min(start, mateStart)
      val maxStart = Math.max(start, mateStart)
      (maxStart - minStart).toInt + sequence.length
    })

    val mate = MateAlignmentProperties(
      mateReferenceContig,
      mateStart,
      inferredInsertSize = Some(insertSize),
      isPositiveStrand = isMatePositiveStrand
    )
    PairedMappedRead(
      makePairedRead(
        chr, start, alignmentQuality, isPositiveStrand, isMateMapped = true,
        Some(mate.referenceContig), Some(mate.start), mate.isPositiveStrand,
        sequence, cigar, mate.inferredInsertSize).read,
      isFirstInPair = true,
      inferredInsertSize = insertSize,
      mate = mate)
  }

  def assertBases(bases1: String, bases2: String) = bases1 should equal(bases2)
  def assertBases(bases1: Iterable[Byte], bases2: String) = Bases.basesToString(bases1) should equal(bases2)

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
    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    (
      loadReads(sc, tumorFile, filters = filters).mappedReads.collect(),
      loadReads(sc, normalFile, filters = filters).mappedReads.collect()
    )
  }

  def loadReads(sc: SparkContext,
                filename: String,
                filters: Read.InputFilters = Read.InputFilters.empty,
                config: Read.ReadLoadingConfig = Read.ReadLoadingConfig.default): ReadSet = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = testDataPath(filename)
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    ReadSet(sc, path, filters = filters, config = config)
  }

  def loadTumorNormalPileup(tumorReads: Seq[MappedRead],
                            normalReads: Seq[MappedRead],
                            locus: Long,
                            reference: ReferenceBroadcast): (Pileup, Pileup) = {
    val contig = tumorReads(0).referenceContig
    assume(normalReads(0).referenceContig == contig)
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
            contig => LociSet.parse(s"$contig:$locus-${locus + 1}")
          )
        )
      ).mappedReads
    val localReads = records.collect
    val actualContig = maybeContig.getOrElse(localReads(0).referenceContig)
    Pileup(
      localReads,
      actualContig,
      locus,
      referenceContigSequence = reference.getContig(actualContig)
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
