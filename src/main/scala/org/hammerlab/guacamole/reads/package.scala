package org.hammerlab.guacamole

import org.hammerlab.guacamole.reference.ReferenceRegion

/**
 * Classes for representing sequenced reads, mapped or unmapped.
 */
package object reads {
  type SampleRegion = ReferenceRegion with HasSampleId
}
