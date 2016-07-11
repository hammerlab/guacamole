package org.hammerlab.guacamole.readsets.iterator.overlaps

import org.hammerlab.guacamole.loci.iterator.{IntersectLociIterator, SkippableLociIterator, UnionLociIterator}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.Position.Locus
import org.hammerlab.guacamole.reference.{Contig, ContigIterator, HasLocus, Position, ReferenceRegion}
import org.hammerlab.magic.iterator.SimpleBufferedIterator

/**
 * Iterator that consumes reads and emits genomic-position-keyed objects.
 *
 * Implementations must define:
 *
 *    - a SkippableLociIterator[T] that emits locus-keyed T's given a contig-restricted read-iterator.
 *    - a function transforming an output T of the above iterator to a final output type U.
 *
 * The two implementations in this file, {{PositionRegionsIterator}} and {{PositionRegionsPerSampleIterator}}, generate
 * an Iterable[R] (resp. PerSample[Iterable[R]]]) for {each non-empty position in @loci} ∪ @forceCallLoci.
 *
 * @param loci loci eligible to be analyzed.
 * @param forceCallLoci loci for which a tuple must be emitted, even if no reads overlapped.
 * @param regions regions to traverse, possibly spanning multiple contigs.
 * @param empty empty T to emit for "forced" loci potentially skipped, e.g. due to no regions overlapping them.
 * @tparam R region type
 * @tparam T loci-keyed intermediate type; a SkippableLociIterator[T] is created for each contig's-worth of R's.
 * @tparam U final output type: tuples of (genomic position, U) are emitted by this class.
 */
abstract class PositionRegionsIteratorBase[R <: ReferenceRegion, T <: HasLocus, U](loci: LociSet,
                                                                                   forceCallLoci: LociSet,
                                                                                   regions: BufferedIterator[R],
                                                                                   empty: Locus => T)
  extends SimpleBufferedIterator[(Position, U)] {

  // Given some regions on a contig, emit locus-keyed objects.
  def newObjIterator(contigRegions: ContigIterator[R]): SkippableLociIterator[T]

  // Final values emitted can be transformed from loci-keyed T objects to some other type U; position-keyed U's will be
  // produced, ultimately.
  def unwrapResult(t: T): U

  var curContigLociIterator: Iterator[T] = _
  var curContig: Contig = _

  def clearContig(): Unit = {
    curContigLociIterator = null
    while (regions.hasNext && regions.head.contig == curContig) {
      regions.next()
    }
  }

  override def _advance: Option[(Position, U)] = {
    if (curContigLociIterator != null && !curContigLociIterator.hasNext) {
      clearContig()
    }

    if (curContigLociIterator == null) {

      if (!regions.hasNext)
        return None

      // We will restrict ourselves to loci and regions on this contig in this iteration of the loop.
      curContig = regions.head.contig

      // Iterator over the loci on this contig allowed by the input LociSet.
      val allowedContigLoci = loci.onContig(curContig).iterator

      // Positions on this contig that we must emit records at, even if the underlying data would otherwise skip them.
      val requiredContigLoci = forceCallLoci.onContig(curContig).iterator

      // Restrict to regions on the current contig.
      val contigRegions = ContigIterator(curContig, regions)

      // Iterator that emits loci based on the provided regions.
      val lociObjs = newObjIterator(contigRegions)

      // 1. intersect the loci allowed by the LociSet with the loci that have reads overlapping them.
      // 2. union the result with the "forced" loci.
      curContigLociIterator =
        new UnionLociIterator(
          requiredContigLoci.map(pos => empty(pos)).buffered,
          new IntersectLociIterator(allowedContigLoci, lociObjs)
        )

      return _advance
    }

    // We can only get here when curContigLociIterator != null && curContigLociIterator.hasNext.
    // We return None in the loop if that can no longer happen, which signals that this iterator is done.
    val obj = curContigLociIterator.next()

    // Add the contig alongside the object's locus as a "key" and unwrap the the object to a value.
    Some(
      Position(curContig, obj.locus) -> unwrapResult(obj)
    )
  }
}

