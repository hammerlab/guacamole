package org.hammerlab.guacamole.variants

import org.hammerlab.genomics.bases.Base.{ A, C, T }
import org.hammerlab.genomics.bases.Bases
import org.hammerlab.guacamole.util.GuacFunSuite

class AlleleSuite
  extends GuacFunSuite {

  test("isVariant") {
    val mismatch = Allele(T, A)
    mismatch.isVariant should be(true)

    val reference = Allele(T, T)
    reference.isVariant should be(false)

    val deletion = Allele(Bases(T, T, T), T)
    deletion.isVariant should be(true)

    val insertion = Allele(T, Bases(T, A, A))
    insertion.isVariant should be(true)
  }

  test("serializing allele") {
    val a1 = Allele(T, A)

    val a1Serialized = serialize(a1)
    val a1Deserialized = deserialize[Allele](a1Serialized)

    assert(a1 === a1Deserialized)

    val a2 = Allele(Bases(T, T, C), A)
    val a2Serialized = serialize(a2)
    val a2Deserialized = deserialize[Allele](a2Serialized)

    assert(a2 === a2Deserialized)
  }

}
