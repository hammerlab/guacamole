package org.hammerlab.guacamole.pileup

import com.esotericsoftware.kryo.Kryo
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord}
import org.hammerlab.guacamole.loci.partitioning.AllLociPartitionerArgs
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.pileup.PileupsRDD._
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.readsets.rdd.ReadsRDDUtil.{TestPileup, simplifyPileup}
import org.hammerlab.guacamole.readsets.rdd.{PartitionedRegions, ReadsRDD, ReadsRDDUtil}
import org.hammerlab.guacamole.readsets.{ContigLengths, ContigLengthsUtil, PartitionedReads, PerSample, ReadSets}
import org.hammerlab.guacamole.reference.Position.Locus
import org.hammerlab.guacamole.reference.{ContigName, ReferenceBroadcast}
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.util.{Bases, GuacFunSuite, KryoTestRegistrar, TestUtil}

import scala.collection.mutable

class PileupsRDDSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Array[IndexedSeq[_]]])
  }
}

class PileupsRDDSuite extends GuacFunSuite with ReadsRDDUtil with ContigLengthsUtil {
  type TestRead = (String, Int, Int, Int)
  type TestReads = Seq[TestRead]
  type TestPos = (String, Int)
  type ExpectedReadIdx = Int
  type ExpectedReadIdxs = Iterable[ExpectedReadIdx]
  type ExpectedReads = String
  type ExpectedReads3 = (ExpectedReads, ExpectedReads, ExpectedReads)
  type ExpectedReadIdxs3 = (ExpectedReadIdxs, ExpectedReadIdxs, ExpectedReadIdxs)

  type ExpectedPosReads = (TestPos, ExpectedReads)

  type ExpectedPosPerSample = (TestPos, ExpectedReads3)

  override def registrar: String = "org.hammerlab.guacamole.pileup.PileupsRDDSuiteRegistrar"

  val contigLengths: ContigLengths =
    makeContigLengths(
      "chr1" -> 200,
      "chr2" -> 300,
      "chr5" -> 400
    )

  val sequenceDictionary =
    SequenceDictionary(
      (for {
        (c, l) <- contigLengths.toSeq
      } yield
        SequenceRecord(c, l)
        ): _*
    )

  val refStrs =
    List(
      ("chr1", 100, "AACGGTTT"),
      ("chr2", 8, "TTG"),
      ("chr2", 102, "AACC"),
      ("chr5", 90, "G")
    )

  val refsMap = mutable.Map[String, mutable.Map[Int, Byte]]()

  for {
    (contig, pos, sequence) <- refStrs
    (base, idx) <- sequence.zipWithIndex
  } {
    refsMap
      .getOrElseUpdate(
        contig,
        mutable.Map[Int, Byte]()
      )
      .getOrElseUpdate(
        pos + idx,
        base.toByte
      )
  }

  lazy val reference: ReferenceBroadcast =
    ReferenceBroadcast(
      (for {
        (contig, basesMap) <- refsMap
        contigLength = contigLengths(contig)
        basesMapBroadcast = sc.broadcast(basesMap.toMap)
      } yield
        contig -> MapBackedReferenceSequence(contigLength.toInt, basesMapBroadcast)
      ).toMap
    )
  def makeReadSets(reads: TestReads, numPartitions: Int): (ReadSets, Seq[MappedRead]) = {
    val (readsets, allReads) = makeReadSets(Vector(reads), numPartitions)
    (readsets, allReads(0))
  }

  def makeReadSets(reads: PerSample[TestReads], numPartitions: Int): (ReadSets, PerSample[Seq[MappedRead]]) = {
    val (readsRDDs, allReads) =
      (for {
        (sampleReads, sampleId) <- reads.zipWithIndex
      } yield {
        val mappedReads =
          for {
            (contig, start, end, num) <- sampleReads
            referenceContig = reference.getContig(contig)
            sequence = Bases.basesToString(referenceContig.slice(start, end))
            i <- 0 until num
          } yield
            TestUtil.makeRead(
              sequence,
              cigar = (end - start).toString + "M",
              start = start,
              chr = contig,
              sampleId = sampleId
            )

        val reads = mappedReads.map(x => x: Read)

        val rdd = sc.parallelize(reads, numPartitions)

        ReadsRDD(rdd, "test", "test", contigLengths) -> mappedReads
      }).unzip

    (ReadSets(readsRDDs, sequenceDictionary), allReads.toVector)
  }

  def makePartitionedReads(readsets: ReadSets,
                           halfWindowSize: Int,
                           maxRegionsPerPartition: Int,
                           numPartitions: Int): PartitionedReads = {
    val args = new AllLociPartitionerArgs {}
    args.parallelism = numPartitions
    args.maxReadsPerPartition = maxRegionsPerPartition

    PartitionedRegions(readsets.allMappedReads, LociSet.all(contigLengths), args, halfWindowSize)
  }

  def checkPileup(contigName: ContigName,
                  locus: Int,
                  reads: Seq[MappedRead],
                  readsStr: ExpectedReads,
                  pileup: TestPileup): Unit = {
    pileup should be(
      (
        contigName,
        locus,
        readsStr
      )
    )
  }

  def testPileups(maxRegionsPerPartition: Int,
                  numPartitions: Iterable[Int],
                  reads: TestReads,
                  expected: Iterable[ExpectedPosReads]): Unit =
    for {
      numPartitions <- numPartitions
    } {
      testPileups(maxRegionsPerPartition, numPartitions, reads, expected)
    }

  def testPileups(maxRegionsPerPartition: Int,
                  numPartitions: Int,
                  reads: TestReads,
                  expected: Iterable[ExpectedPosReads]): Unit = {

    val (readsets, mappedReads) = makeReadSets(reads, numPartitions)

    val partitionedReads = makePartitionedReads(readsets, halfWindowSize = 0, maxRegionsPerPartition, numPartitions)

    val pileups =
      partitionedReads
        .pileups(reference)
        .map(simplifyPileup)
        .collect()

    withClue(s"numPartitions: $numPartitions\n") {
      pileups.length should be(expected.size)

      for {
        (pileup, ((contig, locus), readsStr)) <- pileups.zip(expected)
      } {
        checkPileup(contig, locus, mappedReads, readsStr, pileup)
      }
    }
  }


  {
    // Some tests of the all-sample Pileup-iteration logic.
    val reads =
      List(
        ("chr1", 100, 105, 1),
        ("chr1", 101, 106, 1),
        ("chr2", 8, 9, 1),
        ("chr2", 9, 11, 1),
        ("chr2", 102, 105, 1),
        ("chr2", 103, 106, 10),
        ("chr5", 90, 91, 10)
      )

    val pileups =
      List(
        ("chr1", 100) -> "[100,105)",
        ("chr1", 101) -> "[100,105), [101,106)",
        ("chr1", 102) -> "[100,105), [101,106)",
        ("chr1", 103) -> "[100,105), [101,106)",
        ("chr1", 104) -> "[100,105), [101,106)",
        ("chr1", 105) -> "[101,106)",
        ("chr2",   8) -> "[8,9)",
        ("chr2",   9) -> "[9,11)",
        ("chr2",  10) -> "[9,11)",
        ("chr2", 102) -> "[102,105)",
        ("chr2", 103) -> "[102,105), [103,106)*10",
        ("chr2", 104) -> "[102,105), [103,106)*10",
        ("chr2", 105) -> "[103,106)*10",
        ("chr5",  90) -> "[90,91)*10"
      )

    test("pileups: small") {
      testPileups(
        maxRegionsPerPartition = 10,
        numPartitions = 1 to 2,
        reads.slice(0, 2),
        pileups.filter {
          case (("chr1", _), _) => true
          case _ => false
        }
      )
    }

    test("pileups: large") {
      testPileups(
        maxRegionsPerPartition = 11,
        numPartitions = 1 to 4,
        reads,
        pileups
      )
    }

    test("pileups: drop deepest loci") {
      testPileups(
        maxRegionsPerPartition = 10,
        numPartitions = 1 to 4,
        reads = reads,
        expected = pileups.filter {
          // Loci [103,104] have ≥ 10 depth and get dropped here.
          case (("chr2", locus), _) if 103 <= locus && locus <= 104 => false
          case _ => true
        }
      )
    }
  }

  def testPerSamplePileups(maxRegionsPerPartition: Int,
                           numPartitions: Int,
                           reads: PerSample[TestReads],
                           expected: Iterable[ExpectedPosPerSample]): Unit = {
    val (readsets, mappedReads) = makeReadSets(reads, numPartitions)

    val partitionedReads = makePartitionedReads(readsets, halfWindowSize = 0, maxRegionsPerPartition, numPartitions)

    val pileups: Array[PerSample[(ContigName, Locus, ExpectedReads)]] =
      partitionedReads
        .perSamplePileups(reads.length, reference)
        .map(_.map(simplifyPileup))
        .collect()

    withClue(s"numPartitions: $numPartitions\n") {
      pileups.length should be(expected.size)

      for {
        (pileups, ((contig, locus), (reads1, reads2, reads3))) <- pileups.zip(expected)
        reads = Vector(reads1, reads2, reads3)
      } {
        pileups.length should be(3)

        withClue(s"$contig:$locus:\n") {
          for {
            (((pileup, readsStr), sampleReads), sampleId) <- pileups.zip(reads).zip(mappedReads).zipWithIndex
          } {
            withClue(s"sample: $sampleId\n") {
              checkPileup(contig, locus, sampleReads, readsStr, pileup)
            }
          }
        }
      }
    }
  }

  {
    val sample1Reads =
      List(
        ("chr1", 100, 103, 2),
        ("chr1", 102, 104, 2),
        ("chr1", 106, 107, 1),
        ("chr2",   8,   9, 5),
        ("chr2",  20,  22, 3)
      )

    val sample2Reads =
      List(
        ("chr2",   8,   9, 5),
        ("chr2",  30,  32, 3)
      )

    val sample3Reads =
      List(
        ("chr1", 101, 102, 1),
        ("chr1", 102, 103, 5),
        ("chr1", 104, 105, 3),
        ("chr2",   8,   9, 5),
        ("chr2",  40,  42, 3)
      )

    val pileups: Iterable[ExpectedPosPerSample] =
      List[ExpectedPosPerSample](
        "chr1" -> 100 -> ("[100,103)*2", "", ""),
        "chr1" -> 101 -> ("[100,103)*2", "", "[101,102)"),
        "chr1" -> 102 -> ("[100,103)*2, [102,104)*2", "", "[102,103)*5"),
        "chr1" -> 103 -> ("[102,104)*2", "", ""),
        "chr1" -> 104 -> ("", "", "[104,105)*3"),
        "chr1" -> 106 -> ("[106,107)", "", ""),
        "chr2" ->   8 -> ("[8,9)*5", "[8,9)*5", "[8,9)*5"),
        "chr2" ->  20 -> ("[20,22)*3", "", ""),
        "chr2" ->  21 -> ("[20,22)*3", "", ""),
        "chr2" ->  30 -> ("", "[30,32)*3", ""),
        "chr2" ->  31 -> ("", "[30,32)*3", ""),
        "chr2" ->  40 -> ("", "", "[40,42)*3"),
        "chr2" ->  41 -> ("", "", "[40,42)*3")
      )

    test("per-sample pileups") {
      testPerSamplePileups(
        maxRegionsPerPartition = 20,
        numPartitions = 1,
        reads = Vector(sample1Reads, sample2Reads, sample3Reads),
        expected = pileups
      )
    }
  }
}
