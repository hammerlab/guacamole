package org.hammerlab.guacamole

/**
 * Strategies for filtering reads, filtering bases within reads, and filtering the results of variant calling (called
 * genotypes).
 *
 * For example, we might want to include only reads whose mapping quality is above a threshold, bases
 * whose base quality is above a threshold, and called variants that exceed a certain likelihood score.
 */
package object filters {}
