package org.hammerlab.guacamole.distributed

import org.hammerlab.guacamole.reference.{ContigName, ReferenceRegion}

/**
 * Using an iterator of regions sorted by (contig, start locus), this class exposes a way to get separate iterators
 * over the regions in each contig.
 *
 * For example, given these regions (written as contig:start locus):
 *    chr20:1000,chr20:1500,chr21:200
 *
 * Calling next("chr20") will return an iterator of two regions (chr20:1000 and chr20:1500). After that, calling
 * next("chr21") will give an iterator of one region (chr21:200).
 *
 * Note that you must call next("chr20") before calling next("chr21") in this example. That is, this class does not
 * buffer anything -- it just walks forward in the regions using the iterator you gave it.
 *
 * Also note that after calling next("chr21"), the iterator returned by our previous call to next() is invalidated.
 *
 * @param regionIterator regions, sorted by contig and start locus.
 */
class RegionsByContig[R <: ReferenceRegion](regionIterator: Iterator[R]) {

  private val buffered = regionIterator.buffered

  private var seenContigs = List.empty[ContigName]
  private var curIterator: SingleContigRegionIterator[R] = _

  def next(contigName: ContigName): Iterator[R] = {

    // Drop any remaining regions from the previous contig (which drops them from the underlying regionIterator), as the
    // new SingleContigRegionIterator assumes it receives regionIterator pointing at the start of a fresh contig.
    while (curIterator != null && curIterator.hasNext) curIterator.next()

    // The next element from the iterator should have a contig we haven't seen so far.
    assert(
      buffered.isEmpty || !seenContigs.contains(buffered.head.contigName),
      "Regions are not sorted by contig. Contigs requested so far: %s. Next regions's contig: %s.".format(
        seenContigs.reverse.toString, buffered.head.contigName
      )
    )

    seenContigs ::= contigName

    // Wrap our iterator and return it.
    curIterator = new SingleContigRegionIterator(contigName, buffered)
    curIterator
  }
}

