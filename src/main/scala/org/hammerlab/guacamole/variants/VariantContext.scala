package org.hammerlab.guacamole.variants

import htsjdk.variant.variantcontext.{VariantContext => HTSJDKVariantContext}
import org.hammerlab.genomics.reference.{ContigName, Locus}

object VariantContext {
  def unapply(vc: HTSJDKVariantContext): Option[(ContigName, Locus, Locus)] =
    Some(
      (
        vc.getContig,
        vc.getStart - 1L,
        vc.getEnd.toLong
      )
    )
}
