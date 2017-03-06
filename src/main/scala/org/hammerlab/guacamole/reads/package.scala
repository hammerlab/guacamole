package org.hammerlab.guacamole

import org.hammerlab.genomics.readsets.HasSampleId
import org.hammerlab.genomics.reference.Region

/**
 * Classes for representing sequenced reads, mapped or unmapped.
 */
package object reads {
  type SampleRegion = Region with HasSampleId
}
