package org.hammerlab.guacamole.util

import java.io.{File, FileNotFoundException}
import java.nio.file.Files

import htsjdk.samtools.TextCigarCodec
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, MateAlignmentProperties, PairedRead, Read}
import org.hammerlab.guacamole.readsets.{ReadSets, SampleName}
import org.hammerlab.guacamole.readsets.args.SingleSampleArgs
import org.hammerlab.guacamole.readsets.io.{InputFilters, ReadLoadingConfig}
import org.hammerlab.guacamole.readsets.rdd.ReadsRDD
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.reference.{ContigName, ContigSequence, Locus, ReferenceBroadcast}

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
                    contigStartSequences: Seq[(ContigName, Int, String)],
                    contigLengths: Int = 1000): ReferenceBroadcast = {
    val map = mutable.HashMap[String, ContigSequence]()
    contigStartSequences.foreach({
      case (contig, start, sequence) => {
        val locusToBase: Map[Int, Byte] =
          (for {
            (base, locus) <- Bases.stringToBases(sequence).zipWithIndex
          } yield
            (locus + start) -> base
          ).toMap

        map.put(contig, MapBackedReferenceSequence(contigLengths, sc.broadcast(locusToBase)))
      }
    })

    new ReferenceBroadcast(map.toMap, source=Some("test_values"))
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
    val args = new SingleSampleArgs {}
    args.reads = path
    ReadSets.loadReads(args, sc, filters)._1
  }

  def loadTumorNormalPileup(tumorReads: Seq[MappedRead],
                            normalReads: Seq[MappedRead],
                            locus: Locus,
                            reference: ReferenceBroadcast): (Pileup, Pileup) = {
    val contig = tumorReads(0).contigName
    assume(normalReads(0).contigName == contig)
    (
      Pileup(tumorReads.filter(_.overlapsLocus(locus)), contig, locus, reference.getContig(contig)),
      Pileup(normalReads.filter(_.overlapsLocus(locus)), contig, locus, reference.getContig(contig))
    )
  }

  def loadPileup(sc: SparkContext,
                 filename: String,
                 reference: ReferenceBroadcast,
                 locus: Locus = 0,
                 maybeContig: Option[ContigName] = None): Pileup = {
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
    val actualContig = maybeContig.getOrElse(localReads(0).contigName)
    Pileup(
      localReads,
      actualContig,
      locus,
      reference.getContig(actualContig)
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
