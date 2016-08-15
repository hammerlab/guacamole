package org.hammerlab.guacamole.variants

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.bdgenomics.formats.avro.GenotypeAllele

/**
 * A Genotype is a sequence of alleles of length equal to the ploidy of the organism.
 *
 * A Genotype is for a particular reference locus. Each allele gives the base(s) present on a chromosome at that
 * locus.
 *
 * For example, the possible single-base diploid genotypes are Seq('A', 'A'), Seq('A', 'T') ... Seq('T', 'T').
 * Alleles can also be multiple bases as well, e.g. Seq("AAA", "T")
 *
 */
case class Genotype(alleles: Allele*) {
  /**
   * The ploidy of the organism is the number of alleles in the genotype.
   */
  val ploidy = alleles.size

  lazy val uniqueAllelesCount = alleles.toSet.size

  lazy val getNonReferenceAlleles: Seq[Allele] = {
    alleles.filter(_.isVariant)
  }

  /**
   * Counts alleles in this genotype that are not the same as the specified reference allele.
   *
   * @return Count of non reference alleles
   */
  lazy val numberOfVariantAlleles: Int = getNonReferenceAlleles.size

  /**
   * Returns whether this genotype contains any non-reference alleles for a given reference sequence.
   *
   * @return True if at least one allele is not the reference
   */
  lazy val hasVariantAllele: Boolean = (numberOfVariantAlleles > 0)

  /**
   * Transform the alleles in this genotype to the ADAM allele enumeration.
   * Classifies alleles as Reference or Alternate.
   *
   * @return Sequence of Genotype which are Ref, Alt or OtherAlt.
   */
  lazy val getGenotypeAlleles: Seq[GenotypeAllele] = {
    assume(ploidy == 2)
    val numVariants = numberOfVariantAlleles
    if (numVariants == 0) {
      Seq(GenotypeAllele.Ref, GenotypeAllele.Ref)
    } else if (numVariants > 0 && uniqueAllelesCount == 1) {
      Seq(GenotypeAllele.Alt, GenotypeAllele.Alt)
    } else if (numVariants >= 2 && uniqueAllelesCount > 1) {
      Seq(GenotypeAllele.Alt, GenotypeAllele.OtherAlt)
    } else {
      Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)
    }
  }

  override def toString: String = "Genotype(%s)".format(alleles.map(_.toString).mkString(","))
}

class GenotypeSerializer
    extends Serializer[Genotype]
    with HasAlleleSerializer {
  def write(kryo: Kryo, output: Output, obj: Genotype) = {
    output.writeInt(obj.alleles.length, true)
    obj.alleles.foreach(alleleSerializer.write(kryo, output, _))
  }

  def read(kryo: Kryo, input: Input, klass: Class[Genotype]): Genotype = {
    val numAlleles = input.readInt(true)
    val alleles = (0 to numAlleles).map(i => alleleSerializer.read(kryo, input, classOf[Allele]))
    Genotype(alleles: _*)
  }
}
