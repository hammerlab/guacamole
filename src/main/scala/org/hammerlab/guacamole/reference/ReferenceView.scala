package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.loci.map.LociMap
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference
import org.hammerlab.genomics.reference.{ Contig, ContigLengths, ContigName, Locus, NumLoci }
import org.hammerlab.paths.Path

case class ReferenceView(map: LociMap[ContigSubsequence],
                         source: Option[Path],
                         contigLengths: ContigLengths)
  extends ReferenceGenome {

  override def apply(contigName: ContigName): Contig =
    new Contig {
      val mapContig = map(contigName)

      override val length: NumLoci = contigLengths(contigName)

      override def apply(locus: Locus): Base =
        mapContig.get(locus) match {
          case Some(ContigSubsequence(basesStart, bases)) ⇒
            val idx = (locus - basesStart).toInt
            bases(idx)
          case None ⇒
            throw new IndexOutOfBoundsException(
              s"Position $contigName:$locus unavailable on $contigName"
            )
        }

      override def slice(start: reference.Locus, length: Int): Bases =
        mapContig.get(start) match {
          case Some(ContigSubsequence(basesStart, bases)) ⇒
            val startIdx = (start - basesStart).toInt
            val endIdx = startIdx + length
            if (endIdx > bases.length)
              throw new IndexOutOfBoundsException(
                s"Requested $length bases from $contigName:$start but only ${bases.length} were available"
              )
            bases.slice(startIdx, endIdx)
          case None ⇒
            throw new IndexOutOfBoundsException(
              s"Position $contigName:$start unavailable while requesting $contigName:$start:${start + length}"
            )
        }
    }
}

case class ContigSubsequence(start: Locus, bases: Bases)

object ReferenceView {
  def apply(wrapped: ReferenceGenome, set: LociSet): ReferenceView = {
    val builder = LociMap.newBuilder[ContigSubsequence]
    val contigLengthsBuilder = Map.newBuilder[ContigName, NumLoci]
    for {
      setContig ← set.contigs
      contigName = setContig.name
      refContig = wrapped(contigName)
      range ← setContig.ranges
    } {
      contigLengthsBuilder +=
        contigName → refContig.length

      builder += (
        contigName,
        range.start,
        range.end,
        ContigSubsequence(
          range.start,
          refContig.slice(
            range.start,
            range.length.toInt
          )
        )
      )
    }

    ReferenceView(
      builder.result,
      wrapped.source,
      contigLengthsBuilder.result
    )
  }
}
