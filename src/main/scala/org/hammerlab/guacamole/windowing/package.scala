package org.hammerlab.guacamole

import org.hammerlab.guacamole.reference.Region

/**
 * Step through loci in the genome and at each step consider all the reads (or any object that implements the trait
 * [[Region]]) that overlap the locus.
 */
package object windowing {}
