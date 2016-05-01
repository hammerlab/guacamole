package org.hammerlab.guacamole.variants

import java.io.OutputStream

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.FileAlreadyExistsException
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Genotype => BDGGenotype}
import org.codehaus.jackson.JsonFactory
import org.hammerlab.guacamole.logging.LoggingUtils._

object VariantUtils {
  /**
   * Write out an RDD of Genotype instances to a file.
   *
   * @param args parsed arguments
   * @param genotypes ADAM genotypes (i.e. the variants)
   */
  def writeVariantsFromArguments(args: GenotypeOutputArgs, genotypes: RDD[BDGGenotype]): Unit = {
    val subsetGenotypes = if (args.maxGenotypes > 0) {
      progress(s"Subsetting to ${args.maxGenotypes} genotypes.")
      genotypes.sample(withReplacement = false, args.maxGenotypes, 0)
    } else {
      genotypes
    }
    val outputPath = args.variantOutput.stripMargin
    if (outputPath.isEmpty || outputPath.toLowerCase.endsWith(".json")) {
      val out: OutputStream = if (outputPath.isEmpty) {
        progress("Writing genotypes to stdout.")
        System.out
      } else {
        progress(s"Writing genotypes serially in json format to: $outputPath")
        val filesystem = FileSystem.get(new Configuration())
        val path = new Path(outputPath)
        filesystem.create(path, true)
      }
      val coalescedSubsetGenotypes = if (args.outChunks > 0) subsetGenotypes.coalesce(args.outChunks) else subsetGenotypes
      coalescedSubsetGenotypes.persist()

      // Write each Genotype with a JsonEncoder.
      val schema = BDGGenotype.getClassSchema
      val writer = new GenericDatumWriter[Object](schema)
      val encoder = EncoderFactory.get.jsonEncoder(schema, out)
      val generator = new JsonFactory().createJsonGenerator(out)
      generator.useDefaultPrettyPrinter()
      encoder.configure(generator)
      var partitionNum = 0
      val numPartitions = coalescedSubsetGenotypes.partitions.length
      while (partitionNum < numPartitions) {
        progress(s"Collecting partition ${partitionNum + 1} of $numPartitions")
        val chunk = coalescedSubsetGenotypes.mapPartitionsWithIndex((num, genotypes) => {
          if (num == partitionNum) genotypes else Iterator.empty
        }).collect
        chunk.foreach(genotype => {
          writer.write(genotype, encoder)
          encoder.flush()
        })
        partitionNum += 1
      }
      out.write("\n".getBytes())
      generator.close()
      coalescedSubsetGenotypes.unpersist()
    } else if (outputPath.toLowerCase.endsWith(".vcf")) {
      progress(s"Writing genotypes to VCF file: $outputPath")
      subsetGenotypes.toVariantContext.coalesce(1, shuffle = true).saveAsVcf(outputPath)
    } else {
      progress(s"Writing genotypes to: $outputPath.")
      subsetGenotypes.adamParquetSave(
        outputPath,
        args.blockSize,
        args.pageSize,
        args.compressionCodec,
        args.disableDictionaryEncoding
      )
    }
  }

  /**
   * Perform validation of command line arguments at startup.
   * This allows some late failures (e.g. output file already exists) to be surfaced more quickly.
   */
  def validateArguments(args: GenotypeOutputArgs) = {
    val outputPath = args.variantOutput.stripMargin
    if (outputPath.toLowerCase.endsWith(".vcf")) {
      val filesystem = FileSystem.get(new Configuration())
      val path = new Path(outputPath)
      if (filesystem.exists(path)) {
        throw new FileAlreadyExistsException("Output directory " + path + " already exists")
      }
    }
  }
}
