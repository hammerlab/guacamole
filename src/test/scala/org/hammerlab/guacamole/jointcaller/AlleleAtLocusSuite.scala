package org.hammerlab.guacamole.jointcaller

import org.hammerlab.guacamole.pileup.{ Util ⇒ PileupUtil }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.util.TestUtil.resourcePath
import AlleleAtLocus.variantAlleles

class AlleleAtLocusSuite
  extends GuacFunSuite
    with PileupUtil {

  val celsr1BAMs =
    Vector("normal_0.bam", "tumor_wes_2.bam", "tumor_rna_11.bam")
      .map(name => s"cancer-wes-and-rna-celsr1/$name")

  val b37Chromosome22Fasta = resourcePath("chr22.fa.gz")

  override lazy val reference =
    ReferenceBroadcast(b37Chromosome22Fasta, sc, partialFasta = false)

  test("variantAlleles for low vaf variant allele") {
    val inputs = InputCollection(celsr1BAMs, analytes = Vector("dna", "dna", "rna"))

    val pileups =
      (inputs.normalDNA ++ inputs.tumorDNA).map(
        input =>
          loadPileup(sc, input.path, 46931060, Some("chr22"))
      )

    val possibleAlleles =
      variantAlleles(
        pileups,
        anyAlleleMinSupportingReads = 2,
        anyAlleleMinSupportingPercent = 2,
        maxAlleles = Some(5),
        atLeastOneAllele = false,
        onlyStandardBases = true
      )

    possibleAlleles should equal(Seq(AlleleAtLocus("chr22", 46931061, "G", "A")))
  }
}
