package org.hammerlab.guacamole.commands

import java.io.File

import htsjdk.variant.variantcontext.{ Allele, GenotypeBuilder, VariantContext, VariantContextBuilder }
import htsjdk.variant.vcf.VCFFileReader
import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.hammerlab.guacamole.{ Bases, LociMap, LociSet }
import org.scalatest.Matchers

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

// This test currently does not make any assertions, but outputs a performance comparison. We may want to add assertions
// on the accuracy later.
class SomaticJointCallerIntegrationTests extends GuacFunSuite with Matchers {

  sparkTest("somatic calling on subset of 3-sample cancer patient 1") {
    val outDir = "/tmp/guacamole-somatic-joint-test"
    if (true) {
      if (true) {
        val args = new SomaticJoint.Arguments()
        args.outDir = outDir
        args.referenceFastaPath = referenceFastaPath
        args.referenceFastaIsPartial = true
        // args.loci = "chr1:197069825-197069847"
        args.somaticGenotypePolicy = "trigger"
        args.loci = ((1).until(22).map(i => "chr%d".format(i)) ++ Seq("chrX", "chrY")).mkString(",")

        args.paths = cancerWGS1Bams.toArray
        val forceCallLoci = LociSet.newBuilder
        csvRecords(cancerWGS1ExpectedSomaticCallsCSV).filter(!_.tumor.contains("decoy")).foreach(record => {
          forceCallLoci.put("chr" + record.contig, record.interbaseStart, Some(record.interbaseStart + 1))
        })
        args.forceCallLoci = forceCallLoci.result.truncatedString(100000)

        SomaticJoint.Caller.run(args, sc)
      }

      println("************* CANCER WGS1 SOMATIC CALLS *************")

      compareToCSV(outDir + "/somatic.all_samples.vcf", cancerWGS1ExpectedSomaticCallsCSV)
    }
  }

  sparkTest("germline calling on subset of illumina platinum NA12878") {
    if (true) {
      val resultFile = tempFile(".vcf")
      println(resultFile)

      if (true) {
        val args = new SomaticJoint.Arguments()
        args.out = resultFile
        args.paths = Seq(na12878SubsetBam).toArray
        args.loci = "chr1:0-6700000"
        args.forceCallLociFromFile = na12878ExpectedCallsVCF
        args.referenceFastaPath = chr1PrefixFasta
        SomaticJoint.Caller.run(args, sc)
      }

      println("************* GUACAMOLE *************")
      compareToVCF(resultFile, na12878ExpectedCallsVCF)

      if (false) {
        println("************* UNIFIED GENOTYPER *************")
        compareToVCF(TestUtil.testDataPath(
          "illumina-platinum-na12878/unified_genotyper.vcf"),
          na12878ExpectedCallsVCF)

        println("************* HaplotypeCaller *************")
        compareToVCF(TestUtil.testDataPath(
          "illumina-platinum-na12878/haplotype_caller.vcf"),
          na12878ExpectedCallsVCF)
      }
    }
  }

  // Everything below are helper functions.

  def loadPileup(filename: String, referenceName: String, locus: Long = 0): Pileup = {
    val records = TestUtil.loadReads(sc, filename).mappedReads
    val localReads = records.collect
    Pileup(localReads, referenceName, locus)
  }

  var tempFileNum = 0
  def tempFile(suffix: String): String = {
    tempFileNum += 1
    "/tmp/test-somatic-joint-caller-%d.vcf".format(tempFileNum)
    //val file = File.createTempFile("test-somatic-joint-caller", suffix)
    //file.deleteOnExit()
    //file.getAbsolutePath
  }

  val na12878SubsetBam = TestUtil.testDataPath(
    "illumina-platinum-na12878/NA12878.10k_variants.plus_chr1_3M-3.1M.bam")

  val na12878ExpectedCallsVCF = TestUtil.testDataPath(
    "illumina-platinum-na12878/NA12878.subset.vcf")

  val chr1PrefixFasta = TestUtil.testDataPath("illumina-platinum-na12878/chr1.prefix.fa")
  val referenceFastaPath = TestUtil.testDataPath("hg19.partial.fasta")
  def referenceBroadcast = ReferenceBroadcast(referenceFastaPath, sc, partialFasta = true)

  val cancerWGS1Bams = Seq("normal.bam", "primary.bam", "recurrence.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  val cancerWGS1ExpectedSomaticCallsCSV = TestUtil.testDataPath("cancer-wgs1/variants.csv")

  def vcfRecords(reader: VCFFileReader): Seq[VariantContext] = {
    val results = new ArrayBuffer[VariantContext]
    val iterator = reader.iterator
    while (iterator.hasNext) {
      val value = iterator.next()
      results += value
    }
    results
  }

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

    def toHtsjdVariantContext(reference: ReferenceBroadcast): VariantContext = {
      val uncanonicalizedContig = "chr" + contig

      val (adjustedInterbaseStart, adjustedRef, adjustedAlt) = if (alt.nonEmpty) {
        (interbaseStart, ref, alt)
      } else {
        // Deletion.
        val refSeqeunce = Bases.basesToString(
          reference.getReferenceSequence(
            uncanonicalizedContig, interbaseStart.toInt - 1, interbaseStart.toInt + 1)).toUpperCase
        (interbaseStart - 1, refSeqeunce, refSeqeunce(0) + alt)
      }

      val alleles = Seq(adjustedRef, adjustedAlt).distinct

      def makeHtsjdkAllele(allele: String): Allele = Allele.create(allele, allele == adjustedRef)

      val genotype = new GenotypeBuilder(tumor)
        .alleles(JavaConversions.seqAsJavaList(Seq(adjustedRef, adjustedAlt).map(makeHtsjdkAllele _)))
        .make

      new VariantContextBuilder()
        .chr(uncanonicalizedContig)
        .start(adjustedInterbaseStart + 1) // one based based inclusive
        .stop(adjustedInterbaseStart + 1 + math.max(adjustedRef.length - 1, 0))
        .genotypes(JavaConversions.seqAsJavaList(Seq(genotype)))
        .alleles(JavaConversions.seqAsJavaList(alleles.map(makeHtsjdkAllele _)))
        .make
    }
  }

  def csvRecords(filename: String): Seq[VariantFromVarlensCSV] = {
    Source.fromFile(filename).getLines.map(_.split(",").map(_.trim).toSeq).toList match {
      case header :: records => {
        records.map(record => {
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
        })
      }
      case Nil => throw new IllegalArgumentException("Empty file")
    }
  }

  case class VCFComparison(expected: Seq[VariantContext], experimental: Seq[VariantContext]) {

    val mapExpected = VCFComparison.makeLociMap(expected)
    val mapExperimental = VCFComparison.makeLociMap(experimental)

    val exactMatch = new ArrayBuffer[(VariantContext, VariantContext)]
    val partialMatch = new ArrayBuffer[(VariantContext, VariantContext)]
    val uniqueToExpected = new ArrayBuffer[VariantContext]
    val uniqueToExperimental = new ArrayBuffer[VariantContext]

    VCFComparison.accumulate(expected, mapExperimental, exactMatch, partialMatch, uniqueToExpected)

    {
      // Accumulate result.{exact,partial}Match in throwaway arrays so we don't double count.
      val exactMatch2 = new ArrayBuffer[(VariantContext, VariantContext)]
      val partialMatch2 = new ArrayBuffer[(VariantContext, VariantContext)]

      VCFComparison.accumulate(experimental, mapExpected, exactMatch2, partialMatch2, uniqueToExperimental)
      // assert(exactMatch2.size == exactMatch.size)
      // assert(partialMatch2.size == partialMatch.size)
    }

    def sensitivity = exactMatch.size * 100.0 / expected.size
    def specificity = exactMatch.size * 100.0 / experimental.size

    def summary(): String = {
      Seq(
        "exact match: %,d".format(exactMatch.size),
        "partial match: %,d".format(partialMatch.size),
        "unique to expected: %,d".format(uniqueToExpected.size),
        "unique to experimental: %,d".format(uniqueToExperimental.size),
        "sensitivity: %1.2f%%".format(sensitivity),
        "specificity: %1.2f%%".format(specificity)
      ).mkString("\n")
    }

  }
  object VCFComparison {
    private def accumulate(
      records: Seq[VariantContext],
      map: LociMap[VariantContext],
      exactMatch: ArrayBuffer[(VariantContext, VariantContext)],
      partialMatch: ArrayBuffer[(VariantContext, VariantContext)],
      unique: ArrayBuffer[VariantContext]): Unit = {
      records.foreach(record1 => {
        map.onContig(record1.getContig).get(record1.getStart) match {
          case Some(record2) => {
            if (variantToString(record1) == variantToString(record2)) {
              exactMatch += ((record1, record2))
            } else {
              partialMatch += ((record1, record2))
            }
          }
          case None => unique += record1
        }
      })
    }

    private def makeLociMap(records: Seq[VariantContext]): LociMap[VariantContext] = {
      val builder = LociMap.newBuilder[VariantContext]()
      records.foreach(record => {
        // Switch from zero based inclusive to interbase coordinates.
        builder.put(record.getContig, record.getStart, record.getEnd + 1, record)
      })
      builder.result
    }
  }

  def variantToString(variant: VariantContext, verbose: Boolean = false): String = {
    // We always use the non-hom-ref genotype when there is one.
    val genotypes = (0 until variant.getGenotypes.size).map(variant.getGenotype _)
    val genotype = genotypes.sortBy(_.getType.toString == "HOM_REF").head

    if (verbose) {
      variant.toString
    } else {
      "%s:%d-%d %s > %s %s".format(
        variant.getContig,
        variant.getStart,
        variant.getEnd,
        variant.getReference,
        JavaConversions.collectionAsScalaIterable(variant.getAlternateAlleles).map(_.toString).mkString(","),
        genotype.getType.toString)
    }
  }

  def printSamplePairs(pairs: Seq[(VariantContext, VariantContext)], num: Int = 20): Unit = {
    val sample = pairs.take(num)
    println("Showing %,d / %,d.".format(sample.size, pairs.size))
    sample.zipWithIndex.foreach({
      case (pair, num) => {
        println("(%4d) %20s vs %20s \tDETAILS: %20s vs %20s".format(
          num + 1,
          variantToString(pair._1, false),
          variantToString(pair._2, false),
          variantToString(pair._1, true),
          variantToString(pair._2, true)))
      }
    })
  }

  def printSample(items: Seq[VariantContext], num: Int = 20): Unit = {
    val sample = items.take(num)
    println("Showing %,d / %,d.".format(sample.size, items.size))
    sample.zipWithIndex.foreach({
      case (item, num) => {
        println("(%4d) %20s \tDETAILS: %29s".format(
          num + 1,
          variantToString(item, false),
          variantToString(item, true)))
      }
    })
  }

  def compareToCSV(experimentalFile: String, expectedFile: String): Unit = {
    val readerExperimental = new VCFFileReader(new File(experimentalFile), false)
    val recordsExperimental = vcfRecords(readerExperimental)
    val csvRecordsExpectedWithDecoys = csvRecords(expectedFile)
    val recordsExpected = csvRecordsExpectedWithDecoys
      .filter(!_.tumor.contains("decoy"))
      .map(_.toHtsjdVariantContext(referenceBroadcast))

    println("Experimental calls: %,d. Gold calls: %,d.".format(recordsExperimental.length, recordsExpected.length))

    val comparisonFull = VCFComparison(recordsExpected, recordsExperimental)

    println("Specificity on full: %f".format(comparisonFull.specificity))

    println(comparisonFull.summary)

    println("MISSED CALLS WITH DEPTH")
    printSamplePairs(comparisonFull.partialMatch.filter(
      pair => pair._2.getGenotype(0).isHomRef && pair._2.getGenotype(0).getDP > 5))
    println()

    println("BAD CALLS")
    printSample(comparisonFull.uniqueToExperimental, 400)
    println()
  }

  def compareToVCF(experimentalFile: String, expectedFile: String): Unit = {

    val readerExpected = new VCFFileReader(new File(expectedFile), false)
    val recordsExpected = vcfRecords(readerExpected)
    val reader = new VCFFileReader(new File(experimentalFile), false)
    val recordsGuacamole = vcfRecords(reader)

    println("Experimental calls: %,d. Gold calls: %,d.".format(recordsGuacamole.length, recordsExpected.length))

    def onlyIndels(calls: Seq[VariantContext]): Seq[VariantContext] = {
      calls.filter(call => call.getReference.length != 1 ||
        JavaConversions.collectionAsScalaIterable(call.getAlternateAlleles).exists(_.length != 1))
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
