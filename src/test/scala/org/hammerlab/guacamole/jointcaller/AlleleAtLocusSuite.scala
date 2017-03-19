package org.hammerlab.guacamole.jointcaller

import org.hammerlab.guacamole.data.Celsr1
import org.hammerlab.guacamole.jointcaller.AlleleAtLocus.variantAlleles
import org.hammerlab.guacamole.pileup.{ Util ⇒ PileupUtil }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.test.resources.File

class AlleleAtLocusSuite
  extends GuacFunSuite
    with PileupUtil {

  val b37Chromosome22Fasta = File("chr22.fa.gz")

  override lazy val reference =
    ReferenceBroadcast(b37Chromosome22Fasta, sc, partialFasta = false)

  test("variantAlleles for low vaf variant allele") {
    val inputs = Celsr1.inputs

    val pileups =
      (inputs.normalDNA ++ inputs.tumorDNA).map(
        input ⇒
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
