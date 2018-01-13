package org.hammerlab.guacamole.variants

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.variant.GenotypeRDD
import org.bdgenomics.formats.avro.Sample
import org.hammerlab.cli.args4j.Args
import org.hammerlab.genomics.readsets.{ PerSample, SampleName }
import org.hammerlab.guacamole.commands.GuacCommand
import org.hammerlab.guacamole.logging.LoggingUtils.progress

/**
 * Caller-interface that writes computed variants to disk according to a [[GenotypeOutputArgs]].
 * @tparam ArgsT [[org.hammerlab.cli.args4j.Args]] type.
 * @tparam V [[ReferenceVariant]] type.
 */
trait GenotypeOutputCaller[ArgsT <: Args with GenotypeOutputArgs, V <: ReferenceVariant] extends GuacCommand[ArgsT] {
  override def run(args: ArgsT, sc: SparkContext): Unit = {
    val (variants, sequenceDictionary, sampleNames) = computeVariants(args, sc)

    variants.persist()

    progress("Found %,d variants.".format(variants.count))

    val genotypes = variants.map(_.toBDGGenotype)

    val bdgSamples =
      for {
        sampleName ← sampleNames
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
  }

  def computeVariants(args: ArgsT, sc: SparkContext): (RDD[V], SequenceDictionary, PerSample[SampleName])
}
