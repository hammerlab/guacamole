package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.util.TestUtil.Implicits._
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}

class UnmappedReadSerializerSuite extends GuacFunSuite {

  test("serialize / deserialize unmapped read") {
    val read = UnmappedRead(
      "read1",
      "TCGACCCTCGA",
      Array[Byte]((10 to 20).map(_.toByte): _*),
      isDuplicate = true,
      "some sample name",
      failedVendorQualityChecks = false,
      isPaired = true
    )

    val serialized = serialize(read)
    val deserialized = deserialize[UnmappedRead](serialized)

    // We *should* be able to just use MappedRead's equality implementation, since Scala should implement the equals
    // method for case classes. Somehow, something goes wrong though, and this fails:

    // deserialized should equal(read)

    // So, instead, we'll compare each field ourselves:
    deserialized.name should equal(read.name)
    deserialized.sequence should equal(read.sequence)
    deserialized.baseQualities should equal(read.baseQualities)
    deserialized.isDuplicate should equal(read.isDuplicate)
    deserialized.sampleName should equal(read.sampleName)
    deserialized.failedVendorQualityChecks should equal(read.failedVendorQualityChecks)
    deserialized.isPaired should equal(read.isPaired)
  }

}
