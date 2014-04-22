package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import scala.collection.immutable.NumericRange
import org.bdgenomics.adam.avro.ADAMGenotype
import org.bdgenomics.adam.cli.{ SparkArgs, ParquetArgs, Args4jBase }
import org.kohsuke.args4j.{ Option, Argument }
import org.bdgenomics.guacamole.SlidingReadWindow

/**
 * Interface for variant caller implementations.
 */
trait VariantCaller extends Serializable {
  /**
   * The size of the sliding window (number of bases to either side of a locus) requested by this variant caller
   * implementation.
   *
   * Implementations must override this.
   *
   */
  val halfWindowSize: Long

  /**
   * Given a the samples to call variants for, a [[SlidingReadWindow]], and loci on one contig to call variants at, returns an iterator of
   * genotypes giving the result of variant calling. The [[SlidingReadWindow]] will have the window size requested
   * by this variant caller implementation.
   *
   * Implementations must override this.
   *
   */
  def callVariants(samples: Seq[String], reads: SlidingReadWindow, loci: LociSet.SingleContig): Iterator[ADAMGenotype]
}

/**
 *
 * Interface for creating a variant caller implementation from command line arguments.
 *
 * Each variant caller implementation should have a companion object that implements this interface.
 *
 * If you add a new variant caller, you should also update the [[Guacamole.variantCallers]] list in Guacamole.scala to
 * include it.
 *
 */
trait VariantCallerFactory {

  /** The name of the variant caller, as it will be specified on the command line. */
  val name: String

  /** A short description of the variant caller, for display in the usage info on the command line. */
  val description: String

  /**
   * Parse commandline arguments, and return a pair of the standard guacamole arguments, and a variant caller instance.
   *
   * This is a bit hacky. See an existing variant caller implementation to see how to implement it.
   *
   * @param args the command line arguments, with the first one chopped off. The first argument specifies what variant
   *             caller to call, and is therefore already consumed by Guacamole.
   */
  def fromCommandLineArguments(args: Array[String]): (GuacamoleCommonArguments, VariantCaller)
}
