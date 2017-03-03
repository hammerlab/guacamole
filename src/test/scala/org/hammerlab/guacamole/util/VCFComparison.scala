package org.hammerlab.guacamole.util

import htsjdk.variant.variantcontext.VariantContext
import org.hammerlab.genomics.loci.map.LociMap
import org.hammerlab.genomics.reference.Locus

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

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
  }

  def sensitivity = exactMatch.size * 100.0 / expected.size
  def specificity = exactMatch.size * 100.0 / experimental.size

  def summary: String =
    Seq(
      "exact match: %,d".format(exactMatch.size),
      "partial match: %,d".format(partialMatch.size),
      "unique to expected: %,d".format(uniqueToExpected.size),
      "unique to experimental: %,d".format(uniqueToExperimental.size),
      "sensitivity: %1.2f%%".format(sensitivity),
      "specificity: %1.2f%%".format(specificity)
    )
    .mkString("\n")

}

object VCFComparison {
  private def accumulate(
    records: Seq[VariantContext],
    map: LociMap[VariantContext],
    exactMatch: ArrayBuffer[(VariantContext, VariantContext)],
    partialMatch: ArrayBuffer[(VariantContext, VariantContext)],
    unique: ArrayBuffer[VariantContext]): Unit = {
    records.foreach {
      record1 ⇒
        map(record1.getContig).get(Locus(record1.getStart)) match {
          case Some(record2) ⇒
            if (variantToString(record1) == variantToString(record2))
              exactMatch += ((record1, record2))
            else
              partialMatch += ((record1, record2))
          case None =>
            unique += record1
        }
    }
  }

  private def makeLociMap(records: Seq[VariantContext]): LociMap[VariantContext] = {
    val builder = LociMap.newBuilder[VariantContext]
    records.foreach {
      record =>
        // Switch from zero based inclusive to interbase coordinates.
        builder.put(
          record.getContig,
          Locus(record.getStart),
          Locus(record.getEnd + 1),
          record
        )
    }

    builder.result
  }

  def variantToString(variant: VariantContext, verbose: Boolean = false): String = {
    // We always use the non-hom-ref genotype when there is one.
    val genotypes = (0 until variant.getGenotypes.size).map(variant.getGenotype)
    val genotype = genotypes.minBy(_.getType.toString == "HOM_REF")
    val trigger = Option(variant.getAttribute("TRIGGER"))
    val calledString = if (trigger.isEmpty) {
      // If there is no TRIGGER field (e.g. a VCF from a non-guacamole caller) then we use the genotype.
      if (genotype.isHomRef) "NO_CALL" else "CALLED"
    } else {
      if (trigger.get == "NONE") "NO_CALL" else "CALLED"
    }

    val result = "%s:%d-%d %s > %s %s %s".format(
      variant.getContig,
      variant.getStart,
      variant.getEnd,
      variant.getReference,
      JavaConversions.collectionAsScalaIterable(variant.getAlternateAlleles).map(_.toString).mkString(","),
      genotype.getType.toString,
      calledString) + (if (verbose) " [%s]".format(variant.toString) else "")
    result
  }
}
