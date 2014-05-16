package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole._
import org.apache.spark.{ SparkContext, Logging }
import org.bdgenomics.guacamole.Common.Arguments.{ TumorNormalReads, Output, Base }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.adam.cli.Args4j
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.avro.{ ADAMRecord, ADAMGenotype }
import org.apache.spark.rdd.RDD
import scala.collection.immutable.LongMap
import org.bdgenomics.adam.models.SequenceDictionary

/**
 * Simple subtraction based somatic variant caller
 *
 * This takes two variant callers, calls variants on tumor and normal independently
 * and outputs the variants in the tumor sample BUT NOT the normal sample
 *
 * This assumes that both read sets only contain a single sample, otherwise we should compare
 * on a sample identifier when joining the genotypes
 *
 */
object SomaticSubtractionVariantCaller extends Command with Serializable with Logging {
  override val name = "logodds-somatic"
  override val description = "call somatic variants using a two independent caller on tumor and normal"

  private class Arguments extends Base with Output with TumorNormalReads with DistributedUtil.Arguments {
    @Opt(name = "-log-odds", metaVar = "X", usage = "Make a call if the log odds value is greater than")
    var logOdds: Int = 8
    @Opt(name = "-threshold", metaVar = "X", usage = "Make a call if at least X% of reads support it. Default: 8")
    var threshold: Int = 8
  }

  override def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(args)

    val (mappedNormalReads, normalLoci) = loadReadAndLoci(args.normalReads, sc, args.parallelism)
    val (mappedTumorReads, tumorLoci) = loadReadAndLoci(args.tumorReads, sc, args.parallelism)

    mappedNormalReads.persist()
    mappedTumorReads.persist()

    val caller = (reads: RDD[MappedRead], loci: LociMap[Long]) =>
      DistributedUtil.pileupFlatMap[ADAMGenotype](
        reads,
        loci,
        pileup => BayesianQualityVariantCaller.callVariantsAtLocus(pileup).iterator)

    val genotypes = callSomaticVariants(caller, mappedTumorReads, mappedNormalReads, args.parallelism, Some(tumorLoci), Some(normalLoci))

    mappedTumorReads.unpersist()
    mappedNormalReads.persist()

    Common.writeVariants(args, genotypes)
    DelayedMessages.default.print()
  }

  def loadReadAndLoci(path: String, sc: SparkContext, tasks: Int = 4, lociArg: String = "all"): (RDD[MappedRead], LociMap[Long]) = {
    val (reads, sequenceDictionary) = Read.loadReadRDDAndSequenceDictionaryFromBAM(path, sc, mapped = true, nonDuplicate = true)
    val mappedReads = reads.map(_.getMappedRead).filter(_.mdTag.isDefined)
    (mappedReads, getPartionedLoci(mappedReads, tasks, Some(sequenceDictionary), lociArg))
  }

  def getPartionedLoci(mappedReads: RDD[MappedRead], tasks: Int, sequenceDictionary: Option[SequenceDictionary] = None, lociArg: String = "all"): LociMap[Long] = {
    val loci =
      if (lociArg == "all") {
        // Call at all loci.
        if (sequenceDictionary.isDefined) Common.getLociFromAllContigs(sequenceDictionary.get) else Common.getLociFromReads(mappedReads)
      } else {
        // Call at specified loci.
        LociSet.parse(lociArg)
      }
    val lociPartitions = DistributedUtil partitionLociByApproximateReadDepth (mappedReads, tasks, loci)
    lociPartitions
  }

  /**
   *
   * Call somatic variants by calling variant on the tumor and normal samples independently
   * Once we have variant calls on both samples, we keep the variants that appeared in the tumor sample
   * but not in the normal sample.  This, therefore, DOES NOT include loss-of-heterozygosity variants
   *
   * @param caller Variant caller to use to generate genotypes on each sample, any RDD[ADAMRecord] => RDD[ADANGenotype]
   * @param tumorReads Reads from the tumor sample
   * @param normalReads Reads from the normal sample
   * @return Genotypes that appeared in the tumor sample, but not the normal sample
   */
  def callSomaticVariants(caller: (RDD[MappedRead], LociMap[Long]) => RDD[ADAMGenotype],
                          tumorReads: RDD[MappedRead],
                          normalReads: RDD[MappedRead],
                          tasks: Int = 4,
                          tumorLoci: Option[LociMap[Long]] = None,
                          normalLoci: Option[LociMap[Long]] = None): RDD[ADAMGenotype] = {

    val normalGenotypes = caller(normalReads, normalLoci.getOrElse(getPartionedLoci(normalReads, tasks)))
    log.info("Called %d normal genotypes", normalGenotypes.count)

    val tumorGenotypes = caller(tumorReads, tumorLoci.getOrElse(getPartionedLoci(tumorReads, tasks)))
    log.info("Called %d tumor genotypes", tumorGenotypes.count)

    tumorGenotypes.keyBy(_.variant)
      // Join tumor and normal genotypes on the variant
      .leftOuterJoin(normalGenotypes.keyBy(_.variant))
      // Filter genotypes to
      // (1) those that do not exist in the normal sample
      // (2) those that have different alleles in the normal sample
      // TODO: This filter currently throws out loss-of-heterozygosity variants
      .filter(kv => kv._2._2.isEmpty || kv._2._1.alleles != kv._2._2.get.alleles)
      // Only keep the tumor genotype
      .map(_._2._1)
  }
}