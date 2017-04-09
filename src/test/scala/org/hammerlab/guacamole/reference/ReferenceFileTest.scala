package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.bases.BasesUtil
import org.hammerlab.genomics.reference.test.LociConversions
import org.hammerlab.guacamole.reference.FastaIndex.Entry
import org.hammerlab.paths.Path
import org.hammerlab.test.Suite

class ReferenceFileTest
  extends Suite
    with BasesUtil
    with LociConversions {

  lazy val hg19 = ReferenceFile(Path("/Users/ryan/data/refs/hg19.fasta"))

  test("index") {
    hg19.entries("1") should be(Entry("1", 249250621, 52, 80, 81))
    val chr2 = hg19.entries("2")
    chr2 should be(Entry("2", 243199373, 252366358, 80, 81))
    chr2.offset(10000) should be(252366358 + 10000 / 80 * 81)
    chr2.newlineBytes should be(1)
  }

  test("chr1 N's") {
    hg19("1", 0, 80) should ===("N" * 80)
  }

  test("chr1") {
    hg19("1", 10000, 80) should ===("TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTA")
    hg19("1", 10080, 80) should ===("ACCCTAACCCTAACCCTAACCCTAACCCAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCCTAAC")
    hg19("1", 10160,  1) should ===("C")
    hg19("1", 10161,  1) should ===( "C")
    hg19("1", 10162,  1) should ===(  "T")
    hg19("1", 10163, 80) should ===(   "AACCCTAACCCTAACCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCC" +
                                    "TAA")
    hg19("1", 10243, 77) should ===(   "CCCTAAACCCTAAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCAACCCCAACCCCAACCCCAACCCCAACCC")
  }

  test("chr2 across lines") {
    hg19("2", 9998, 170) should ===(
      bases(
        """NN
          |CGTATCCCACACACCACACCCACACACCACACCCACACACACCCACACCCACACCCACACACACCACACCCACACACCAC
          |ACCCACACCCACACACCACACCCACACCACACCCACACACCACACACCACACCCACACCCACACACACCACACCCACACA
          |CCACACCC"""
      )
    )
  }

  lazy val chr2length = hg19("2").length

  test("invalid read past end of contig") {
    intercept[ContigLengthException] { hg19("2", chr2length - 3, 4) }
  }

  test("read end of contig") {
    hg19("2", chr2length - 100, 100) should ===(
      bases(
        """NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN
          |NNNNNNNNNNNNNNNNNNNN"""
      )
    )
  }

  def bases(str: String) =
    str
      .stripMargin
      .split('\n')
      .mkString("")
}
