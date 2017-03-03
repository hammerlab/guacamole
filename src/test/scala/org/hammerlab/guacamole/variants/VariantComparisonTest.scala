package org.hammerlab.guacamole.variants

import java.io.File

import htsjdk.variant.variantcontext.{ GenotypeBuilder, VariantContextBuilder, Allele ⇒ HTSJDKAllele, VariantContext ⇒ HTSJDKVariantContext }
import htsjdk.variant.vcf.VCFFileReader
import org.hammerlab.genomics.reference.Locus
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.Bases.basesToString
import org.hammerlab.guacamole.util.VCFComparison

import scala.collection.JavaConversions.{ collectionAsScalaIterable, seqAsJavaList }
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import scala.math.max

case class VariantFromVarlensCSV(
    genome: String,
    contig: String,
    interbaseStart: Int,
    interbaseEnd: Int,
    ref: String,
    alt: String,
    tumor: String,
    normal: String,
    validation: String) {

  def toHtsjdVariantContext(reference: ReferenceBroadcast): HTSJDKVariantContext = {
    val uncanonicalizedContig = "chr" + contig

    val (adjustedInterbaseStart, adjustedRef, adjustedAlt) =
      if (alt.nonEmpty) {
        (interbaseStart, ref, alt)
      } else {
        val start = Locus(interbaseStart - 1)
        val length = ref.length
        // Deletion.
        val refSequence =
          basesToString(
            reference
            .getReferenceSequence(
              uncanonicalizedContig,
              start,
              length
            )
          ).toUpperCase

        (interbaseStart - 1, refSequence, refSequence(0) + alt)
      }

    val alleles = Seq(adjustedRef, adjustedAlt).distinct

    def makeHtsjdkAllele(allele: String): HTSJDKAllele = HTSJDKAllele.create(allele, allele == adjustedRef)

    val genotype =
      new GenotypeBuilder(tumor)
        .alleles(
          seqAsJavaList(
            Seq(
              adjustedRef,
              adjustedAlt
            )
            .map(makeHtsjdkAllele)
          )
        )
        .make

    new VariantContextBuilder()
      .chr(uncanonicalizedContig)
      .start(adjustedInterbaseStart + 1) // one based based inclusive
      .stop(adjustedInterbaseStart + 1 + max(adjustedRef.length - 1, 0))
      .genotypes(seqAsJavaList(Seq(genotype)))
      .alleles(seqAsJavaList(alleles.map(makeHtsjdkAllele)))
      .make
  }
}

trait VariantComparisonTest {

  def printSamplePairs(pairs: Seq[(HTSJDKVariantContext, HTSJDKVariantContext)], num: Int = 20): Unit = {
    val sample = pairs.take(num)
    println("Showing %,d / %,d.".format(sample.size, pairs.size))
    sample.zipWithIndex.foreach {
      case (pair, num) ⇒
        println("(%4d) %20s vs %20s \tDETAILS: %20s vs %20s".format(
          num + 1,
          VCFComparison.variantToString(pair._1, verbose = false),
          VCFComparison.variantToString(pair._2, verbose = false),
          VCFComparison.variantToString(pair._1, verbose = true),
          VCFComparison.variantToString(pair._2, verbose = true)))
    }
  }

  def printSample(items: Seq[HTSJDKVariantContext], num: Int = 20): Unit = {
    val sample = items.take(num)
    println("Showing %,d / %,d.".format(sample.size, items.size))
    sample.zipWithIndex.foreach {
      case (item, num) =>
        println("(%4d) %20s \tDETAILS: %29s".format(
          num + 1,
          VCFComparison.variantToString(item, verbose = false),
          VCFComparison.variantToString(item, verbose = true)))
    }
  }

  def vcfRecords(reader: VCFFileReader): Seq[HTSJDKVariantContext] = {
    val results = new ArrayBuffer[HTSJDKVariantContext]
    val iterator = reader.iterator
    while (iterator.hasNext) {
      val value = iterator.next()
      results += value
    }
    results
  }

  def csvRecords(filename: String): Seq[VariantFromVarlensCSV] =
    Source
      .fromFile(filename)
      .getLines
      .map(_.split(",").map(_.trim).toSeq)
      .toList match {
        case header :: records ⇒
          records.map {
            record ⇒
              val fields = header.zip(record).toMap
              VariantFromVarlensCSV(
                fields("genome"),
                fields("contig"),
                fields("interbase_start").toInt,
                fields("interbase_end").toInt,
                fields("ref"),
                fields("alt"),
                fields("tumor"),
                fields("normal"),
                fields("validation"))
          }
        case Nil ⇒
          throw new IllegalArgumentException("Empty file")
    }

  /**
   * Compute specificity and sensitivity of an experimental call set when compared against a CSV of known variants
   * @param experimentalFile VCF file of called experimental variants
   * @param expectedFile CSV file of expected variants
   * @param referenceBroadcast Reference genome
   * @param samplesToCompare Sample names to compare against in the expected variants file
   */
  def compareToCSV(experimentalFile: String,
                   expectedFile: String,
                   referenceBroadcast: ReferenceBroadcast,
                   samplesToCompare: Set[String]): Unit = {
    val readerExperimental = new VCFFileReader(new File(experimentalFile), false)
    val recordsExperimental = vcfRecords(readerExperimental)
    val csvRecordsExpectedWithDecoys = csvRecords(expectedFile)
    val recordsExpected =
      csvRecordsExpectedWithDecoys
        .filter(record ⇒ samplesToCompare.contains(record.tumor))
        .map(_.toHtsjdVariantContext(referenceBroadcast))

    println("Experimental calls: %,d. Gold calls: %,d.".format(recordsExperimental.length, recordsExpected.length))

    val comparisonFull = VCFComparison(recordsExpected, recordsExperimental)

    println("Specificity on full: %f".format(comparisonFull.specificity))

    println(comparisonFull.summary)

    println("MISSED CALLS")
    printSamplePairs(comparisonFull.partialMatch.filter(pair => pair._2.getAttribute("TRIGGER") == "NONE"))
    println()

    println("BAD CALLS")
    printSample(comparisonFull.uniqueToExperimental, 400)
    println()

    println("EXACT MATCHES")
    printSamplePairs(comparisonFull.exactMatch, 400)
  }

  def compareToVCF(experimentalFile: String, expectedFile: String): Unit = {

    val readerExpected = new VCFFileReader(new File(expectedFile), false)
    val recordsExpected = vcfRecords(readerExpected)
    val reader = new VCFFileReader(new File(experimentalFile), false)
    val recordsGuacamole = vcfRecords(reader)

    println("Experimental calls: %,d. Gold calls: %,d.".format(recordsGuacamole.length, recordsExpected.length))

    def onlyIndels(calls: Seq[HTSJDKVariantContext]): Seq[HTSJDKVariantContext] = {
      calls.filter(
        call =>
          call.getReference.length != 1 ||
            collectionAsScalaIterable(call.getAlternateAlleles).exists(_.length != 1)
      )
    }

    val comparisonFull = VCFComparison(recordsExpected, recordsGuacamole)
    val comparisonPlatinumOnly = VCFComparison(
      recordsExpected.filter(_.getAttributeAsString("metal", "") == "platinum"),
      recordsGuacamole)
    val comparisonFullIndels = VCFComparison(onlyIndels(recordsExpected), onlyIndels(recordsGuacamole))

    println("Sensitivity on platinum: %f".format(comparisonPlatinumOnly.sensitivity))
    println("Specificity on full: %f".format(comparisonFull.specificity))

    println(comparisonFull.summary)

    println("MISSED CALLS IN PLATINUM WITH DEPTH")
    printSamplePairs(comparisonPlatinumOnly.partialMatch.filter(
      pair => pair._2.getGenotype(0).isHomRef && pair._2.getGenotype(0).getDP > 5))
    println()

    println("BAD CALLS")
    printSamplePairs(comparisonFull.partialMatch.filter(
      pair => !pair._2.getGenotype(0).isHomRef))
    println()

    println("INDEL PERFORMANCE")
    println(comparisonFullIndels.summary)

    println("INDEL EXACT MATCHES")
    printSamplePairs(comparisonFullIndels.exactMatch)

    println("INDEL MISSED CALLS WITH DEPTH")
    printSamplePairs(comparisonFullIndels.partialMatch.filter(
      pair => pair._2.getGenotype(0).isHomRef && pair._2.getGenotype(0).getDP > 5))
    println()

    println("INDEL BAD CALLS")
    printSamplePairs(comparisonFullIndels.partialMatch.filter(
      pair => !pair._2.getGenotype(0).isHomRef))
    println()
  }
}
