package org.hammerlab.guacamole.variants

import java.io.OutputStream

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.rdd.RDD
import org.apache.spark.{ HashPartitioner, SparkContext }
import org.bdgenomics.adam.rdd.variant.GenotypeRDD
import org.bdgenomics.formats.avro.{ Genotype ⇒ BDGGenotype }
import org.bdgenomics.utils.cli.ParquetArgs
import org.codehaus.jackson.JsonFactory
import org.hammerlab.commands.Args
import org.hammerlab.genomics.readsets.args.base.PrefixedPathsBase
import org.hammerlab.genomics.readsets.args.path.{ UnprefixedPath, UnprefixedPathOptionHandler }
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.kohsuke.args4j.{ Option ⇒ Args4jOption }

/**
 * Arguments for writing output genotypes.
 *
 * Supports writing VCF, JSON, and Parquet formats.
 */
trait GenotypeOutputArgs
  extends ParquetArgs {

  self: Args with PrefixedPathsBase ⇒

  @Args4jOption(
    name = "--out",
    metaVar = "VARIANTS_OUT",
    aliases = Array("-o"),
    required = false,
    handler = classOf[UnprefixedPathOptionHandler],
    usage = "Variant output path. If not specified, print to screen."
  )
  var _outputPathOpt: Option[UnprefixedPath] = None
  def outputPathOpt = _outputPathOpt.map(_.buildPath)

  @Args4jOption(
    name = "--out-chunks",
    required = false,
    usage = "When writing out to json format, number of chunks to coalesce the genotypes RDD into."
  )
  var outChunks: Int = 1

  @Args4jOption(
    name = "--max-genotypes",
    required = false,
    usage = "Maximum number of genotypes to output. 0 (default) means output all genotypes."
  )
  var maxGenotypes: Int = 0

  /**
   * Perform validation of command line arguments at startup.
   * This allows some late failures (e.g. output file already exists) to be surfaced more quickly.
   */
  override def validate(sc: SparkContext): Unit = {
    outputPathOpt foreach {
      outputPath ⇒
        if (outputPath.exists) {
          throw new FileAlreadyExistsException(s"Output directory $outputPath already exists")
        }
    }
  }

  /**
   * Write out an RDD of Genotype instances to a file.
   *
   * @param genotypes ADAM genotypes (i.e. the variants)
   */
  def writeVariants(genotypes: GenotypeRDD): Unit = {

    val subsetGenotypes =
      if (maxGenotypes > 0) {
        progress(s"Subsetting to $maxGenotypes genotypes.")
        genotypes.copy(rdd = genotypes.rdd.sample(withReplacement = false, maxGenotypes, 0))
      } else {
        genotypes
      }

    writeSortedSampledVariants(subsetGenotypes)
  }

  private def writeSortedSampledVariants(genotypes: GenotypeRDD): Unit = {

    outputPathOpt match {
      case Some(outputPath) if outputPath.extension != "json" ⇒
        if (outputPath.extension == "vcf") {
          progress(s"Writing genotypes to VCF file: $outputPath")
          val variantsRDD = genotypes.toVariantContextRDD
          val sortedCoalescedRDD =
            variantsRDD
              .rdd
              .keyBy(
                v ⇒
                  (
                    v.variant.variant.getContigName,
                    v.variant.variant.getStart,
                    v.variant.variant.getEnd
                  )
              )
              .repartitionAndSortWithinPartitions(new HashPartitioner(1))
              .values

          variantsRDD
            .copy(rdd = sortedCoalescedRDD)
            .saveAsVcf(outputPath)

        } else {
          progress(s"Writing genotypes to: $outputPath")
          genotypes.saveAsParquet(
            outputPath,
            blockSize,
            pageSize,
            compressionCodec,
            disableDictionaryEncoding
          )
        }
      case _ ⇒
        writeJSONVariants(genotypes.rdd)
    }
  }

  private def writeJSONVariants(genotypes: RDD[BDGGenotype]): Unit = {
    val out: OutputStream =
      outputPathOpt match {
        case Some(outputPath) ⇒
          progress(s"Writing genotypes as JSON to: $outputPath")
          outputPath.outputStream
        case _ ⇒
          progress("Writing genotypes to stdout.")
          System.out
      }

    val coalescedGenotypes =
      if (outChunks > 0)
        genotypes.coalesce(outChunks)
      else
        genotypes

    coalescedGenotypes.persist()

    // Write each Genotype with a JsonEncoder.
    val schema = BDGGenotype.getClassSchema
    val writer = new GenericDatumWriter[Object](schema)
    val encoder = EncoderFactory.get.jsonEncoder(schema, out)
    val generator = new JsonFactory().createJsonGenerator(out)
    generator.useDefaultPrettyPrinter()
    encoder.configure(generator)
    var partitionNum = 0
    val numPartitions = coalescedGenotypes.partitions.length
    while (partitionNum < numPartitions) {
      progress(s"Collecting partition ${partitionNum + 1} of $numPartitions")

      val chunk =
        coalescedGenotypes
          .mapPartitionsWithIndex { (num, genotypes) ⇒
            if (num == partitionNum)
              genotypes
            else
              Iterator.empty
          }
          .collect

      chunk.foreach { genotype ⇒
        writer.write(genotype, encoder)
        encoder.flush()
      }

      partitionNum += 1
    }

    out.write("\n".getBytes())
    generator.close()
    coalescedGenotypes.unpersist()
  }
}
