package org.hammerlab.guacamole.variants

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.variation.GenotypeRDD
import org.bdgenomics.formats.avro.Sample
import org.hammerlab.guacamole.commands.SparkCommand
import org.hammerlab.guacamole.logging.DelayedMessages
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.{PerSample, SampleName}

/**
 * Caller-interface that writes computed variants to disk according to a [[GenotypeOutputArgs]].
 * @tparam Args [[org.hammerlab.guacamole.commands.Args]] type.
 * @tparam V [[ReferenceVariant]] type.
 */
trait GenotypeOutputCaller[Args <: GenotypeOutputArgs, V <: ReferenceVariant] extends SparkCommand[Args] {
  override def run(args: Args, sc: SparkContext): Unit = {
    val (variants, sequenceDictionary, sampleNames) = computeVariants(args, sc)

    variants.persist()

    progress("Found %,d variants.".format(variants.count))

    val genotypes = variants.map(_.toBDGGenotype)

    val bdgSamples =
      for {
        sampleName <- sampleNames
      } yield
        Sample
          .newBuilder
          // Must match ReferenceVariant.sampleName
          .setSampleId(sampleName)
          // Mostly extraneous
          .setName(sampleName)
          .build()

    val genotypesRDD = GenotypeRDD(genotypes, sequenceDictionary, bdgSamples)

    args.writeVariants(genotypesRDD)

    DelayedMessages.default.print()
  }

  def computeVariants(args: Args, sc: SparkContext): (RDD[V], SequenceDictionary, PerSample[SampleName])
}
