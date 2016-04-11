package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.SequenceDictionary
import org.hammerlab.guacamole.{ContigLengths, PerSample}
import org.hammerlab.guacamole.reads.{Read, ReadLoadingConfig}

/**
 * A `ReadSets` contains reads from multiple inputs, and SequenceDictionary / contig-length information merged from
 * them.
 */
case class ReadSets(readsRDDs: PerSample[ReadsRDD],
                    sequenceDictionary: SequenceDictionary,
                    contigLengths: ContigLengths) extends PerSample[ReadsRDD] {
  override def length: Int = readsRDDs.length
  override def apply(idx: Int): ReadsRDD = readsRDDs(idx)

  val mappedReads = readsRDDs.map(_.mappedReads)
}

object ReadSets {
  /**
    * Load reads from multiple files, merging their sequence dictionaries and verifying that they are consistent.
    */
  def apply(sc: SparkContext,
            filenames: Seq[String],
            filters: Read.InputFilters = Read.InputFilters.empty,
            contigLengthsFromDictionary: Boolean = true,
            config: ReadLoadingConfig = ReadLoadingConfig.default): ReadSets = {

    val (readRDDs, sequenceDictionaries) =
      filenames.map(filename => {
        Read.loadReadRDDAndSequenceDictionary(
          filename,
          sc,
          filters = filters,
          config
        )
      }).unzip

    val sequenceDictionary = mergeSequenceDictionaries(filenames, sequenceDictionaries)

    val contigLengths: ContigLengths = {
      if (contigLengthsFromDictionary) {
        getContigLengthsFromSequenceDictionary(sequenceDictionary)
      } else {
        sc.union(readRDDs)
          .flatMap(_.asMappedRead)
          .map(read => read.referenceContig -> read.end)
          .reduceByKey(math.max)
          .collectAsMap()
          .toMap
      }
    }

    ReadSets(readRDDs.map(ReadsRDD(_)).toVector, sequenceDictionary, contigLengths)
  }

  def mergeSequenceDictionaries(filenames: Seq[String], dicts: Seq[SequenceDictionary]): SequenceDictionary = {
    val records =
      (for {
        (filename, dict) <- filenames.zip(dicts)
        record <- dict.records
      } yield {
        filename -> record
      })
      .groupBy(_._2.name)
      .values
      .map(values => {
        val (filename, record) = values.head

        // Verify that all records for a given contig are equal.
        values.tail.toList.filter(_._2 != record) match {
          case Nil => {}
          case mismatched =>
            throw new IllegalArgumentException(
              (
                s"Conflicting sequence records for ${record.name}:" ::
                s"$filename: $record" ::
                mismatched.map { case (otherFile, otherRecord) => s"${otherFile}: $otherRecord" }
              ).mkString("\n\t")
            )
        }

        record
      })

    new SequenceDictionary(records.toVector).sorted
  }

  /**
    * Construct a map from contig name -> length of contig, using a SequenceDictionary.
    */
  def getContigLengthsFromSequenceDictionary(sequenceDictionary: SequenceDictionary): ContigLengths = {
    val builder = Map.newBuilder[String, Long]
    for {
      record <- sequenceDictionary.records
    } {
      builder += ((record.name.toString, record.length))
    }
    builder.result
  }

  /**
   * Given arguments for two sets of reads (tumor and normal), return a pair of (tumor, normal) read sets.
   *
   * @param args parsed arguments
   * @param sc spark context
   * @param filters input filters to apply
   */
  def loadTumorNormalReadsFromArguments(args: TumorNormalReadsArgs,
                                        sc: SparkContext,
                                        filters: Read.InputFilters): (ReadsRDD, ReadsRDD, ContigLengths) = {

    val ReadSets(readsets, _, contigLengths) =
      ReadSets(
        sc,
        Seq(args.tumorReads, args.normalReads),
        filters,
        !args.noSequenceDictionary,
        ReadLoadingConfigArgs.fromArguments(args)
      )

    (readsets(0), readsets(1), contigLengths)
  }
}
