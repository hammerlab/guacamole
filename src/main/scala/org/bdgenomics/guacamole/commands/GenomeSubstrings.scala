package org.bdgenomics.guacamole.commands

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.cli.{ Args4j, ParquetArgs, Args4jBase }
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.bdgenomics.guacamole.reads.MappedRead
import org.bdgenomics.guacamole.{ DistributedUtil, Common, Command }
import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.{ Option => Opt }

import scala.tools.nsc.io.File

class GenomeSubstringsArgs extends Args4jBase with ParquetArgs with Serializable {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM data to return stats for", index = 0)
  val inputPath: String = null

  @Argument(required = true, metaVar = "MAXLEN", usage = "Maximum substring length to store", index = 1)
  val maxLen: Int = 5

  @Opt(name = "--print-top", metaVar = "N", usage = "Print the most frequently occuring <N> substrings, with counts")
  var printTopN: Int = 100

  @Opt(name = "--print-bottom", metaVar = "N", usage = "Print the least frequently occuring <N> substrings, with counts")
  var printBottomN: Int = 100

  @Opt(name = "--out-file", usage = "Write the substrings and their counts as text to this file")
  var outfile: String = null
}

case class FragmentWithWindow(fragment: NucleotideContigFragment,
                              nextBasesOpt: Option[CharSequence],
                              maxLen: Int) extends Serializable {
  val substrings: Iterator[CharSequence] = (fragment.getFragmentSequence + nextBasesOpt.getOrElse("")).sliding(maxLen)

  val substringsWithLoci: Iterator[(CharSequence, (CharSequence, Long))] =
    for {
      (str, idx) <- substrings.zipWithIndex
      locus = fragment.getFragmentStartPosition + idx
    } yield (str, (fragment.getContig.getContigName, locus))
}

case class FragmentWithWindowOpts(fragmentOpt: Option[NucleotideContigFragment] = None,
                                  nextBasesOpt: Option[CharSequence] = None) extends Serializable

object GenomeSubstrings extends Command {
  val name = "genome-substrings"
  val description = "Count occurrences of all subsequences of a genome that are up to a certain length"

  def run(rawArgs: Array[String]): Unit = {
    val args = Args4j[GenomeSubstringsArgs](rawArgs)
    val sc = Common.createSparkContext(appName = Some(name))

    val refContigFragments: RDD[NucleotideContigFragment] = sc.adamLoad(args.inputPath)

    val fragmentsAndNeighbors: RDD[((CharSequence, Int), FragmentWithWindowOpts)] =
      refContigFragments.flatMap(fragment => {
        val curFragKey = (fragment.getContig.getContigName, fragment.getFragmentNumber.toInt)

        val prevFragKeyOpt =
          if (fragment.getFragmentNumber > 0)
            Some((fragment.getContig.getContigName, fragment.getFragmentNumber - 1))
          else
            None

        val prevFragTupleOpt =
          prevFragKeyOpt.map(
            _ -> FragmentWithWindowOpts(
              nextBasesOpt = Some(
                fragment.getFragmentSequence.subSequence(
                  0,
                  args.maxLen - 1
                )
              )
            )
          )

        (
          Some(curFragKey -> FragmentWithWindowOpts(Some(fragment))) ::
          prevFragTupleOpt ::
          Nil
        ).flatten
      })

    val numFrags = fragmentsAndNeighbors.count()

    val substringsWithCounts: RDD[(CharSequence, Long)] =
      fragmentsAndNeighbors.groupByKey.flatMap {
        case ((contigName, fragmentIdx), fragmentOpts) => {
          val fragment = fragmentOpts.flatMap(_.fragmentOpt) match {
            case fragment :: Nil => fragment
            case Nil =>
              throw new Exception(
                "Found no fragment for (%s,%d)".format(contigName, fragmentIdx)
              )
            case fragments =>
              throw new Exception(
                "Found %d fragments for (%s,%d)".format(fragments.size, contigName, fragmentIdx)
              )
          }

          val nextBasesOpt = fragmentOpts.flatMap(_.nextBasesOpt) match {
            case nextBases :: Nil => {
              assert(fragmentIdx + 1 < fragment.getNumberOfFragmentsInContig)
              Some(nextBases)
            }
            case Nil => {
              assert(fragmentIdx + 1 == fragment.getNumberOfFragmentsInContig)
              None
            }
            case prevBaseses =>
              throw new Exception(
                "Found %d fragments for (%s,%d)".format(prevBaseses.size, contigName, fragmentIdx)
              )
          }

          FragmentWithWindow(fragment, nextBasesOpt, args.maxLen).substrings.map(_ -> 1L)
        }
      }.reduceByKey(_ + _)

    val numSubstrings = substringsWithCounts.count()

    val ACGTSubstrings = substringsWithCounts.filter(_._1.forall(ch => "ACGT".exists(_ == ch)))
    val numACGTSubstrings = ACGTSubstrings.count()

    val NSubstrings = substringsWithCounts.filter(_._1.exists(_ == 'N'))
    val numNSubstrings = NSubstrings.count()

    val numNStartSubstrings = NSubstrings.filter(_._1.startsWith("N")).count()
    val numNEndSubstrings = NSubstrings.filter(_._1.endsWith("N")).count()

    val nonACGTNSubstrings = substringsWithCounts.filter(_._1.exists(ch => !"ACGTN".exists(_ == ch)))
    val numNonACGTNSubstrings = nonACGTNSubstrings.count()

    args.printTopN = math.min(args.printTopN, (numSubstrings + 1) / 2).toInt
    args.printBottomN = math.min(args.printBottomN, numSubstrings - args.printTopN).toInt

    val (topN, bottomN) =
      if (args.printTopN + args.printBottomN >= numSubstrings)
        (numSubstrings.toInt, 0)
      else
        (args.printTopN, args.printBottomN)

    val substringsForPrinting =
      if (bottomN > 0)
        ACGTSubstrings
      else
        substringsWithCounts

    val topSubstrings = substringsForPrinting.top(topN)(Ordering.by((k: (CharSequence, Long)) => k._2))

    if (args.outfile != null) {
      File(args.outfile).printlnAll(
        substringsWithCounts.sortBy(_._2, ascending = false).map(p => "%s: %d".format(p._1, p._2)).collect(): _*
      )
    }

    val bottomSubstrings =
      if (bottomN > 0)
        substringsForPrinting.takeOrdered(bottomN)(Ordering.by((k: (CharSequence, Long)) => k._2))
      else
        Array[(CharSequence, Long)]()

    println(
      """
        |%d substrings of length %d:
        |   - contiaining only ACGT:  %d
        |   - containing N:           %d (%d at the beginning, %d in the middle, %d at the end)
        |   - containing [^ACGTN]:    %d
        |%s
        |%s
        |%s
      """.stripMargin.trim.format(
        numSubstrings, args.maxLen,
        numACGTSubstrings,
        numNSubstrings, numNStartSubstrings, numNEndSubstrings, numNSubstrings - numNStartSubstrings - numNEndSubstrings,
        numNonACGTNSubstrings,
        if (bottomN > 0) "\nThe top %d and bottom %d:".format(topN, bottomN) else "",
        topSubstrings.map(p => "%s: %d".format(p._1, p._2)).mkString("\n"),
        bottomSubstrings.reverse.map(p => "%s: %d".format(p._1, p._2)).mkString("\n")
      )
    )

    println("YAYYY: %d".format(numFrags))
  }
}
