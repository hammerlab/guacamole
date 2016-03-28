package org.hammerlab.guacamole.commands.jointcaller

import java.util

import htsjdk.samtools.SAMSequenceDictionary
import htsjdk.variant.vcf._
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.rdd.variation.ADAMVCFOutputFormat
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.seqdoop.hadoop_bam.{ VCFOutputFormat, VariantContextWritable, VCFFormat }
import org.hammerlab.guacamole.Common
import scala.collection.JavaConversions

/**
 * Contains methods to operate on an RDD of calls, i.e. MultipleAllelesEvidenceAcrossSamples
 *
 */
object MultipleAllelesEvidenceRddFunctions {
  /**
   * Converts an RDD of MultipleAllelesEvidenceAcrossSamples to HTSJDK VariantContexts
   * and saves to disk as VCF.
   *
   * @param filePath The filepath to save to.
   * @param sequenceDictionary
   * @param sortOnSave Whether to sort before saving. Sort is run after coalescing.
   *                   Default is false (no sort).
   * @param coalesceTo Optionally coalesces the RDD down to _n_ partitions. Default is none.
   */
  def saveAsVcf(filePath: String,
                rdd: RDD[MultipleAllelesEvidenceAcrossSamples],
                sequenceDictionary: SAMSequenceDictionary,
                sortOnSave: Boolean = false,
                coalesceTo: Option[Int] = None,
                filteredInputs: Seq[Input],
                parameters: Parameters,
                reference: ReferenceBroadcast,
                includePooledNormal: Option[Boolean] = None,
                includePooledTumor: Option[Boolean] = None): Unit = {
    rdd.cache()
    def createHeader(sequenceDictionary: SAMSequenceDictionary,
                     parameters: Parameters,
                     includePooledNormal: Option[Boolean] = None,
                     includePooledTumor: Option[Boolean] = None): VCFHeader = {

      val headerLines = new util.HashSet[VCFHeaderLine]()
      headerLines.add(new VCFFormatHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype"))
      headerLines.add(new VCFFormatHeaderLine("AD", VCFHeaderLineCount.R, VCFHeaderLineType.Integer,
        "Allelic depths for the ref and alt alleles"))
      headerLines.add(new VCFFormatHeaderLine("PL", VCFHeaderLineCount.G, VCFHeaderLineType.Integer,
        "Phred scaled genotype likelihoods"))
      headerLines.add(new VCFFormatHeaderLine("DP", 1, VCFHeaderLineType.Integer, "Total depth"))
      headerLines.add(new VCFFormatHeaderLine("GQ", 1, VCFHeaderLineType.Integer, "Genotype quality"))

      // nonstandard
      headerLines.add(new VCFFormatHeaderLine("RL", 1, VCFHeaderLineType.String, "Unnormalized log10 genotype posteriors"))
      headerLines.add(new VCFFormatHeaderLine("VAF", 1, VCFHeaderLineType.Integer, "Variant allele frequency (percent)"))
      headerLines.add(new VCFFormatHeaderLine("TRIGGERED", 1, VCFHeaderLineType.Integer, "Did this sample trigger a call"))
      headerLines.add(new VCFFormatHeaderLine("ADP", 1, VCFHeaderLineType.String, "allelic depth as num postiive strand / num total"))

      // INFO
      headerLines.add(new VCFInfoHeaderLine("TRIGGER", 1, VCFHeaderLineType.String,
        "Which likelihood ratio test triggered call: POOLED and/or INDIVIDUAL"))

      val header = new VCFHeader(headerLines,
        JavaConversions.seqAsJavaList(
          filteredInputs.map(_.sampleName)
            ++ (if (includePooledNormal == Some(true)) Seq("pooled_normal") else Seq.empty)
            ++ (if (includePooledTumor == Some(true)) Seq("pooled_tumor") else Seq.empty)))
      header.setSequenceDictionary(sequenceDictionary)
      header.setWriteCommandLine(true)
      header.setWriteEngineHeaders(true)
      header.addMetaDataLine(new VCFHeaderLine("Caller", "Guacamole"))
      parameters.asStringPairs.foreach(kv => {
        header.addMetaDataLine(new VCFHeaderLine("parameter." + kv._1, kv._2))
      })

      header
    }

    val actuallyIncludePooledNormal = includePooledNormal.getOrElse(filteredInputs.count(_.normalDNA) > 1)
    val actuallyIncludePooledTumor = includePooledTumor.getOrElse(filteredInputs.count(_.tumorDNA) > 1)

    val vcfFormat = VCFFormat.inferFromFilePath(filePath)
    assert(vcfFormat == VCFFormat.VCF, "BCF not yet supported") // TODO: Add BCF support

    Common.progress(s"Writing $vcfFormat file to $filePath")

    // Initialize global header object required by Hadoop VCF Writer
    //val header : List[String] = getCallsetSamples()
    val header: VCFHeader = createHeader(sequenceDictionary, parameters, includePooledNormal, includePooledTumor)
    val bcastHeader = rdd.context.broadcast(header)
    val mp = rdd.mapPartitionsWithIndex((idx, iter) => {
      Common.progress(s"Setting header for partition $idx")
      synchronized {
        // perform map partition call to ensure that the VCF header is set on all
        // nodes in the cluster; see:
        // https://github.com/bigdatagenomics/adam/issues/353,
        // https://github.com/bigdatagenomics/adam/issues/676
        ADAMVCFOutputFormat.clearHeader()
        ADAMVCFOutputFormat.setHeader(bcastHeader.value)
        Common.progress(s"Set VCF header for partition $idx")
      }
      Iterator[Int]()
    }).count()

    // force value check, ensure that computation happens
    if (mp != 0) {
      Common.progress("ERROR: Had more than 0 elements after map partitions call to set VCF header across cluster.")
    }

    ADAMVCFOutputFormat.clearHeader()
    ADAMVCFOutputFormat.setHeader(bcastHeader.value) //
    Common.progress("Set VCF header on driver")

    // convert the variants to htsjdk vc

    val gatkVCs: RDD[VariantContextWritable] = rdd.flatMap(_.alleleEvidences.map(
      evidence => {
        val vcw = new VariantContextWritable
        vcw.set(VCFOutput.makeHtsjdVariantContext(evidence,
          InputCollection(filteredInputs), actuallyIncludePooledNormal, actuallyIncludePooledTumor, reference))
        vcw
      }))

    // coalesce the rdd if requested
    val coalescedVCs = coalesceTo.fold(gatkVCs)(p => gatkVCs.repartition(p))

    // sort if requested
    val withKey = if (sortOnSave) {
      coalescedVCs.keyBy(v => ReferencePosition(v.get.getContig(), v.get.getStart()))
        .sortByKey()
        .map(kv => (new LongWritable(kv._1.pos), kv._2))
    } else {
      coalescedVCs.keyBy(v => new LongWritable(v.get.getStart))
    }

    // save to disk
    val conf = rdd.context.hadoopConfiguration
    conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, vcfFormat.toString)
    withKey.saveAsNewAPIHadoopFile(
      filePath,
      classOf[LongWritable], classOf[VariantContextWritable], classOf[ADAMVCFOutputFormat[LongWritable]],
      conf
    )

    Common.progress("Wrote %d records to %s".format(gatkVCs.count(), filePath))
    rdd.unpersist()
  }

}
