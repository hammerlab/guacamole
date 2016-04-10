package org.hammerlab.guacamole.readsets

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.SequenceDictionary
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.reads.Read

case class ReadSets(readsRDDs: PerSample[ReadsRDD],
                    sequenceDictionary: SequenceDictionary,
                    contigLengths: ContigLengths) extends PerSample[ReadsRDD] {
  override def length: Int = readsRDDs.length
  override def apply(idx: Int): ReadsRDD = readsRDDs(idx)

  val mappedReads = readsRDDs.map(_.mappedReads)
}

object ReadSets {
  /**
    * Load reads from multiple files, verifying that their sequence dictionaries match.
    */
  def apply(sc: SparkContext,
            filenames: Seq[String],
            filters: Read.InputFilters = Read.InputFilters.empty,
            contigLengthsFromDictionary: Boolean = true,
            config: Read.ReadLoadingConfig = Read.ReadLoadingConfig.default): ReadSets = {

    val readsAndDicts =
      filenames.map(filename => {

        Read.loadReadRDDAndSequenceDictionary(
          filename,
          sc,
          filters = filters,
          config
        )
      })

    val (readRDDs, sequenceDictionaries) = readsAndDicts.unzip

    val sequenceDictionary = sequenceDictionaries.head
    sequenceDictionaries.tail.foreach(sd =>
      assert(
        sd == sequenceDictionary,
        s"Sequence dictionaries differ:\n${sequenceDictionaries.mkString("\n\n")}"
      )
    )

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

  /**
    * Construct a map from contig name -> length of contig, using a SequenceDictionary.
    */
  def getContigLengthsFromSequenceDictionary(sequenceDictionary: SequenceDictionary): ContigLengths = {
    val builder = Map.newBuilder[String, Long]
    sequenceDictionary.records.foreach(record => builder += ((record.name.toString, record.length)))
    builder.result
  }

}
