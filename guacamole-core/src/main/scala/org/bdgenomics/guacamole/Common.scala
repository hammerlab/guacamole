package org.bdgenomics.guacamole

import org.bdgenomics.adam.cli.{ SparkArgs, ParquetArgs, Args4jBase }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.adam.avro.{ ADAMGenotype, ADAMRecord }
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.Util._
import org.bdgenomics.adam.predicates.LocusPredicate
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMContext

/**
 * Collection of functions that are useful to multiple variant calling implementations, and specifications of command-
 * line arguments that they use.
 *
 */
object Common extends Logging {
  object Arguments {
    /** Common argument(s) we always want.*/
    trait Base extends Args4jBase with ParquetArgs with SparkArgs {
      @Opt(name = "-debug", usage = "If set, prints a higher level of debug output.")
      var debug = false
    }

    /** Argument for accepting a set of loci. */
    trait Loci extends Base {
      @Opt(name = "-loci", usage = "Loci at which to call variants. Format: contig:start-end,contig:start-end,...")
      var loci: String = ""
    }

    /** Argument for accepting a single set of reads (for non-somatic variant calling). */
    trait Reads extends Base {
      @Opt(name = "-reads", metaVar = "X", required = true, usage = "Aligned reads")
      var reads: String = ""
    }

    /** Arguments for accepting two sets of reads (tumor + normal). */
    trait TumorNormalReads extends Base {
      @Opt(name = "-reads-normal", metaVar = "X", required = true, usage = "Aligned reads: normal")
      var readsNormal: String = ""

      @Opt(name = "-reads-tumor", metaVar = "X", required = true, usage = "Aligned reads: tumor")
      var readTumor: String = ""
    }

    /** Argument for writing output genotypes. */
    trait Output extends Base {
      @Opt(name = "-out", metaVar = "VARIANTS_OUT", required = true, usage = "Variant output")
      var variantOutput: String = ""
    }

    /** Arguments for accepting a reference genome. */
    trait Reference extends Base {
      @Opt(required = false, name = "-reference", usage = "ADAM or FASTA reference genome data")
      var referenceInput: String = ""

      @Opt(required = false, name = "-fragment_length", usage = "Sets maximum fragment length. Default value is 10,000. Values greater than 1e9 should be avoided.")
      var fragmentLength: Long = 10000L
    }
  }

  /**
   * Given arguments for a single set of reads, and a spark context, return an RDD of reads.
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param mapped if true, will filter out non-mapped reads
   * @param nonDuplicate if true, will filter out duplicate reads.
   * @return
   */
  def loadReads(args: Arguments.Reads, sc: SparkContext, mapped: Boolean = true, nonDuplicate: Boolean = true): RDD[ADAMRecord] = {
    var reads: RDD[ADAMRecord] = sc.adamLoad(args.reads, Some(classOf[LocusPredicate]))
    progress("Loaded %d reads.".format(reads.count))
    if (mapped) reads = reads.filter(read => read.readMapped && read.referenceName != null && read.referenceLength > 0)
    if (nonDuplicate) reads = reads.filter(read => !read.duplicateRead)
    if (mapped || nonDuplicate) {
      progress("Filtered to %d %s reads".format(
        reads.count, (if (mapped) "mapped " else "") + (if (nonDuplicate) "non-duplicate" else "")))
    }
    reads
  }

  /**
   * If the user specifies a -loci argument, parse it out and return the LociSet. Otherwise, construct a LociSet that
   * includes all the loci spanned by the reads.
   * @param args parsed arguments
   * @param reads RDD of ADAM reads to use if the user didn't specify loci in the arguments.
   */
  def loci(args: Arguments.Loci, reads: RDD[ADAMRecord]): LociSet = {
    val result = {
      if (args.loci.isEmpty) {
        // Call at all loci.
        val contigsAndLengths = reads.map(read => (read.referenceName.toString, read.referenceLength)).distinct.collect
        assume(contigsAndLengths.map(_._1).distinct.length == contigsAndLengths.length,
          "Some contigs have different lengths in reads: " + contigsAndLengths.toString)
        LociSet(contigsAndLengths.toSeq.map({ case (contig, length) => (contig, 0.toLong, length.toLong) }))
      } else {
        // Call at specified loci.
        LociSet.parse(args.loci)
      }
    }
    progress("Considering %d loci across %d contig(s).".format(result.count, result.contigs.length))
    result
  }

  /**
   * Write out an RDD of ADAMGenotype instances to a file.
   * @param args parsed arguments
   * @param genotypes ADAM genotypes (i.e. the variants)
   */
  def writeVariants(args: Arguments.Output, genotypes: RDD[ADAMGenotype]): Unit = {
    log.info("Writing calls to disk.")
    genotypes.adamSave(args.variantOutput,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionary)
  }

  /**
   * Return a spark context.
   * @param args parsed arguments
   */
  def createSparkContext(args: SparkArgs): SparkContext = {
    SerializationUtil.setupContextProperties()
    ADAMContext.createSparkContext(
      "guacamole",
      args.spark_master,
      args.spark_home,
      args.spark_jars,
      args.spark_env_vars,
      args.spark_add_stats_listener,
      args.spark_kryo_buffer_size)
  }
}
