package org.hammerlab.guacamole.variants

import com.esotericsoftware.kryo.Kryo
import org.hammerlab.guacamole.util.Bases.{A, C, T}
import org.hammerlab.guacamole.util.{GuacFunSuite, KryoTestRegistrar}
import org.scalatest.Matchers

class AlleleSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Allele], new AlleleSerializer)
  }
}

class AlleleSuite extends GuacFunSuite with Matchers {

  override def registrar = classOf[AlleleSuiteRegistrar].getCanonicalName

  test("isVariant") {
    val mismatch = Allele(Seq(T), Seq(A))
    mismatch.isVariant should be(true)

    val reference = Allele(Seq(T), Seq(T))
    reference.isVariant should be(false)

    val deletion = Allele(Seq(T, T, T), Seq(T))
    deletion.isVariant should be(true)

    val insertion = Allele(Seq(T), Seq(T, A, A))
    insertion.isVariant should be(true)

  }

  test("serializing allele") {
    val a1 = Allele(Seq(T), Seq(A))

    val a1Serialized = serialize(a1)
    val a1Deserialized = deserialize[Allele](a1Serialized)

    assert(a1 === a1Deserialized)

    val a2 = Allele(Seq(T, T, C), Seq(A))
    val a2Serialized = serialize(a2)
    val a2Deserialized = deserialize[Allele](a2Serialized)

    assert(a2 === a2Deserialized)
  }

}
