package org.hammerlab.guacamole.filters.pileup

import org.hammerlab.guacamole.pileup.PileupElement

/**
 * Filter to remove pileups which may produce multi-allelic variant calls.
 * These are generally more complex and more difficult to call
 */
object MultiAllelicPileupFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param maxPloidy number of alleles to expect (> maxPloidy would mean multiple possible alternates) default: 2
   * @return Empty sequence if there are > maxPloidy possible allelee, otherwise original set of elements
   */
  def apply(elements: Seq[PileupElement], maxPloidy: Int = 2): Seq[PileupElement] =
    if (elements.map(_.allele).toSet.size > maxPloidy)
      Seq.empty
    else
      elements
}
