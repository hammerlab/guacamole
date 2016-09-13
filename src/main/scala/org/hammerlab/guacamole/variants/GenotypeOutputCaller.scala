package org.hammerlab.guacamole.variants

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.commands.SparkCommand
import org.hammerlab.guacamole.logging.DelayedMessages
import org.hammerlab.guacamole.logging.LoggingUtils.progress

/**
 * Caller-interface that writes computed variants to disk according to a [[GenotypeOutputArgs]].
 * @tparam Args [[org.hammerlab.guacamole.commands.Args]] type.
 * @tparam V [[ReferenceVariant]] type.
 */
trait GenotypeOutputCaller[Args <: GenotypeOutputArgs, V <: ReferenceVariant] extends SparkCommand[Args] {
  override def run(args: Args, sc: SparkContext): Unit = {
    val genotypes = computeGenotypes(args, sc)

    genotypes.persist()

    progress("Found %,d genotypes.".format(genotypes.count))

    val adamGenotypes = genotypes.map(_.toBDGGenotype)

    args.writeVariantsFromArguments(adamGenotypes)

    DelayedMessages.default.print()
  }

  def computeGenotypes(args: Args, sc: SparkContext): RDD[V]
}
