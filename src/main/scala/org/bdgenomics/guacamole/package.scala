package org.bdgenomics

/**
 * Guacamole is a framework for writing variant callers on the Apache Spark platform. Several variant callers are
 * implemented in the [[org.bdgenomics.guacamole.commands]] package. The remaining packages implement a library of
 * functionality used by these callers.
 *
 * To get started, take a look at the code for [[org.bdgenomics.guacamole.commands.GermlineThreshold]] for a
 * pedagogical example of a variant caller, then see [[org.bdgenomics.guacamole.pileup.Pileup]] to understand how we
 * work with pileups. The [[org.bdgenomics.guacamole.commands.SomaticStandard]] caller is a more
 * sophisticated caller (for the somatic setting) that gives an example of most of the functionality in the rest of the
 * library.
 */
package object guacamole {}