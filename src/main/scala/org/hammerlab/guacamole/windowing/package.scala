package org.hammerlab.guacamole

import org.hammerlab.guacamole.reference.ReferenceRegion

/**
 * Step through loci in the genome and at each step consider all the reads (or any object that implements the trait
 * [[ReferenceRegion]]) that overlap the locus.
 */
package object windowing {}
