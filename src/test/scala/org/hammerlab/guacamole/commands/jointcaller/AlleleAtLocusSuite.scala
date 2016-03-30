package org.hammerlab.guacamole.commands.jointcaller
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.scalatest.Matchers

class AlleleAtLocusSuite extends GuacFunSuite with Matchers {
  val celsr1BAMs = Seq("normal_0.bam", "tumor_wes_2.bam", "tumor_rna_11.bam").map(
    name => TestUtil.testDataPath("cancer-wes-and-rna-celsr1/" + name))

  val b37Chromosome22Fasta = TestUtil.testDataPath("chr22.fa.gz")
  def b37Chromosome22Reference = {
    ReferenceBroadcast(b37Chromosome22Fasta, sc, partialFasta = false)
  }

  sparkTest("AlleleAtLocus.variantAlleles for low vaf variant allele") {
    val inputs = InputCollection(celsr1BAMs, analytes = Seq("dna", "dna", "rna"))
    val pileups = (inputs.normalDNA ++ inputs.tumorDNA).map(input =>
      TestUtil.loadPileup(sc, input.path, b37Chromosome22Reference, 46931060, Some("chr22")))

    val possibleAlleles = AlleleAtLocus.variantAlleles(
      pileups,
      anyAlleleMinSupportingReads = 2,
      anyAlleleMinSupportingPercent = 2,
      maxAlleles = Some(5),
      atLeastOneAllele = false,
      onlyStandardBases = true)

    possibleAlleles should equal(Seq(AlleleAtLocus("chr22", 46931061, "G", "A")))
  }
}
