package org.hammerlab.guacamole.loci.set

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.Kryo
import org.hammerlab.guacamole.util.{GuacFunSuite, KryoTestRegistrar}

class SerializerSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    // "a closure that includes a LociSet" parallelizes some Range[Long]s.
    kryo.register(Class.forName("scala.math.Numeric$LongIsIntegral$"))

    // "make an RDD[LociSet] and an RDD[Contig]" collects some Strings.
    kryo.register(classOf[Array[String]])
  }
}

class SerializerSuite extends GuacFunSuite {

  override def registrar = "org.hammerlab.guacamole.loci.set.SerializerSuiteRegistrar"

  test("make an RDD[LociSet]") {

    val sets =
      List(
        "",
        "empty:20-20,empty2:30-30",
        "20:100-200",
        "21:300-400",
        "with_dots._and_..underscores11:900-1000",
        "X:5-17,X:19-22,Y:50-60",
        "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120"
      ).map(LociSet(_))

    val rdd = sc.parallelize(sets)
    val result = rdd.map(_.toString).collect.toSeq
    result should equal(sets.map(_.toString))
  }

  test("make an RDD[LociSet], and an RDD[Contig]") {
    val sets =
      List(
        "",
        "empty:20-20,empty2:30-30",
        "20:100-200",
        "21:300-400",
        "X:5-17,X:19-22,Y:50-60",
        "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120"
      ).map(LociSet(_))

    val rdd = sc.parallelize(sets)

    val result = rdd.map(set => {
      set.onContig("21").contains(5)          // no op
      val ranges = set.onContig("21").ranges  // no op
      set.onContig("20").toString
    }).collect.toSeq

    result should equal(sets.map(_.onContig("20").toString))
  }

  test("a closure that includes a LociSet") {
    val set = LociSet("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120,empty:10-10")
    val setBC = sc.broadcast(set)
    val rdd = sc.parallelize(0L until 1000L)
    val result = rdd.filter(i => setBC.value.onContig("chr21").contains(i)).collect
    result should equal(100L until 200)
  }

  test("java serialization") {
    val loci = LociSet("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120,empty:10-10")

    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)

    oos.writeObject(loci)
    oos.close()

    val bytes = baos.toByteArray
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)

    val loci2 = ois.readObject().asInstanceOf[LociSet]

    loci2 should be(loci)
  }
}
