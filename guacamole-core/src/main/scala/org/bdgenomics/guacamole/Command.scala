package org.bdgenomics.guacamole

/**
 *
 * Interface for running a variant caller from command line arguments.
 *
 * Instead of structuring Guacamole as a "framework" that e.g. feeds reads to variant callers, we instead give the
 * variant callers control of execution and let them do anything they want. We provide a library of common functions
 * for them to use as appropriate.
 *
 * If you add a new command, you should also update the [[Guacamole.variantCallers]] list in Guacamole.scala to
 * include it.
 *
 */
trait Command {
  /** The name of the variant caller, as it will be specified on the command line. */
  val name: String

  /** A short description of the variant caller, for display in the usage info on the command line. */
  val description: String

  /**
   * Run the variant caller.
   *
   * @param args the command line arguments, with the first one chopped off. The first argument specifies what variant
   *             caller to call, and is therefore already consumed by Guacamole.
   */
  def run(args: Array[String]): Unit
}
