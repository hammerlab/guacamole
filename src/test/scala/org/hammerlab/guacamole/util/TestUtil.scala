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

import java.io.{ File, FileNotFoundException }
import java.util.UUID

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.{ IKryoRegistrar, KryoInstantiator, KryoPool }
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{ MateAlignmentProperties, ReadPair, MappedRead, Read }
import org.hammerlab.guacamole.{ Bases, GuacamoleKryoRegistrator, ReadSet }
import org.scalatest._

import scala.math._

object TestUtil extends Matchers {

  object Implicits {
    implicit def basesToString = Bases.basesToString _
    implicit def stringToBases = Bases.stringToBases _
  }

  def tmpFileName(suffix: String): String = {
    "TestUtil." + UUID.randomUUID() + suffix
  }

  // As a hack to run a single unit test, you can set this to the name of a test to run only it. See the top of
  // DistributedUtilSuite for an example.
  var runOnly: String = ""

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

  def makeRead(sequence: String,
               cigar: String,
               mdtag: String,
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
      cigarString = cigar,
      mdTagString = mdtag,
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
    mdTag: String = "12"): ReadPair[MappedRead] = {

    val qualityScoreString = sequence.map(x => '@').mkString

    ReadPair(
      Read(
        sequence,
        cigarString = cigar,
        start = start,
        referenceContig = chr,
        mdTagString = mdTag,
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
            inferredInsertSize = None,
            isMatePositiveStrand = isMatePositiveStrand
          )
        )
        else
          None
    )
  }

  def assertBases(bases1: Iterable[Byte], bases2: String) = Bases.basesToString(bases1) should equal(bases2)

  def testDataPath(filename: String): String = {
    val resource = ClassLoader.getSystemClassLoader.getResource(filename)
    if (resource == null) throw new RuntimeException("No such test data file: %s".format(filename))
    resource.getFile
  }

  def loadReads(sc: SparkContext, filename: String): ReadSet = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = testDataPath(filename)
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    ReadSet(sc, path, requireMDTagsOnMappedReads = false)
  }

  def loadTumorNormalReads(sc: SparkContext,
                           tumorFile: String,
                           normalFile: String): (Seq[MappedRead], Seq[MappedRead]) = {
    val filters = Read.InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    (loadReads(sc, tumorFile, filters = filters).mappedReads.collect(), loadReads(sc, normalFile, filters = filters).mappedReads.collect())
  }

  def loadReads(sc: SparkContext,
                filename: String,
                filters: Read.InputFilters = Read.InputFilters.empty): ReadSet = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = testDataPath(filename)
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    ReadSet(sc, path, requireMDTagsOnMappedReads = false, filters = filters)
  }

  def loadTumorNormalPileup(tumorReads: Seq[MappedRead],
                            normalReads: Seq[MappedRead],
                            locus: Long): (Pileup, Pileup) = {
    (Pileup(tumorReads, locus), Pileup(normalReads, locus))
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
