guacamole
=========
[![Build Status](https://travis-ci.org/hammerlab/guacamole.svg?branch=master)](https://travis-ci.org/hammerlab/guacamole)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/guacamole/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/guacamole?branch=master)

Guacamole is a framework for variant calling, i.e. identifying DNA mutations
from [Next Generation Sequencing][seq] data. It currently includes a toy
germline (non-cancer) variant caller as well as two somatic variant callers for
finding cancer mutations.  Most development effort has gone into somatic
calling.

The emphasis is on a readable codebase that can be readily understood and
adapted for experimentation.

Guacamole is written in Scala using the [Apache Spark][spark] engine for
distributed processing. It can run on a single computer or on a Hadoop cluster.

Guacamole supports reading aligned reads as:
 * [BAM or SAM files][sambam]
 * [Parquet files][parquet] of [bdg-formats][] [AlignmentRecords][]
 
It can write the called genotypes as:
 * [VCF files][]
 * Parquet files of bdg-formats [Genotypes][].

Guacamole uses ideas and some functionality from [ADAM][], and takes
inspiration from the [Avocado][] project.

For hacking Guacamole, see our [code docs][].

## Building

Building Guacamole requires [Apache Maven][maven] (version 3.0.4 or higher).

You'll need Guacamole and its dependencies on your classpath to run, which you can do by building one assembly JAR:

```bash
mvn package -Puber -DskipTests
```

or building one JAR containing just Guacamole's classes and another second containing all transitive dependencies:

```bash
# Build a JAR with just Guacamole classes (and one necessary shaded dependency).
mvn package -DskipTests

# Same as above.
mvn package -Pguac -DskipTests

# Build a shaded JAR with all the rest of Guacamole's transitive dependencies.
mvn package -Pdeps -DskipTests

# Build both "guac" and "deps" JARs at once.
mvn package -Pguac,deps -DskipTests
```

## Running
`scripts/guacamole` is a wrapper around `spark-submit` and makes it easy to run locally or on a cluster:

```bash
scripts/guacamole somatic-joint \
    src/test/resources/synth1.normal.100k-200k.withmd.bam \
    src/test/resources/synth1.tumor.100k-200k.withmd.bam \
    --reference /path/to/your/b37.fasta \
    --out /tmp/out.vcf 
```

Change the reference fasta above to point to a local copy of the B37 human
reference.

This calls germline and somatic variants from some small test BAMs using the
*somatic-joint* variant caller, which works with any number of tumor/normal
samples from the same patient. It takes around 2 minutes to run on my 3.1 GHz,
16gb memory Macbook.

Try:

```bash
scripts/guacamole -h
```
for a list of implemented variant callers, or

```bash
scripts/guacamole <caller> -h
```

for help on a particular variant caller.

### Spark Configuration
All Spark configuration, including whether to run in local- or cluster-mode, comes from the Spark properties file(s) provided via the `$GUAC_SPARK_CONFS` environment variable.

This variable is interpreted as a comma-separated list of [Spark properties files](http://spark.apache.org/docs/1.6.1/configuration.html#dynamically-loading-spark-properties).

In all cases, it will be seeded with the contents of [conf/kryo](conf/kryo):

```
spark.serializer          org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator    org.hammerlab.guacamole.kryo.Registrar
```

Additionally, if `$GUAC_SPARK_CONFS` is not set, [conf/local](conf/local) will be used:

```
spark.master  local[1]
spark.driver.memory 4g
```

See [the conf/ directory](conf/) for more example configuration blocks that can be mixed in.

### Running on a Hadoop Cluster
Guacamole currently builds against Spark 1.6.1 and Hadoop 2.7.0, though it will likely run fine with versions close to those.

For example, you could run with cluster- and GC-logging configs from the [conf/](conf/) directory like so:

```bash
export GUAC_SPARK_CONFS=conf/cluster,conf/gc
scripts/guacamole \
  somatic-standard \
    --normal-reads /hdfs/path/to/normal.bam \
    --tumor-reads /hdfs/path/to/tumor.bam \
    --reference /local/path/to/reference.fasta \
    --out /tmp/out.vcf
```

Note that `--reference` must be a local path, while the normal/tumor BAMs and output-VCF paths above point into HDFS.

# Running the test suite
Guacamole contains many small BAMs for testing. The full test suite can be run with:

```
mvn test
```

# Is this ready for production use?

Not currently. Everything here is experimental. Please use a standard tool if
you need accurate variant calls.

# License

Guacamole is released under an [Apache 2.0 license](LICENSE.txt).

YourKit is kindly supporting this open source project with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
[Java](http://www.yourkit.com/java/profiler/index.jsp) and [.NET](http://www.yourkit.com/.net/profiler/index.jsp) applications.
![image](https://cloud.githubusercontent.com/assets/455755/4988560/97757f12-6935-11e4-9270-f5fc42f9b585.png)

[seq]: http://en.wikipedia.org/wiki/DNA_sequencing
[spark]: http://spark.apache.org/
[sambam]: http://genomicsandhealth.org/our-work/work-products/file-formats-sambam
[parquet]: http://parquet.incubator.apache.org/
[bdg-formats]: https://github.com/bigdatagenomics/bdg-formats
[alignmentrecords]: https://github.com/bigdatagenomics/bdg-formats/blob/master/src/main/resources/avro/bdg.avdl#L60
[vcf files]: http://genomicsandhealth.org/our-work/work-products/file-formats-vcfbcf
[genotypes]: https://github.com/bigdatagenomics/bdg-formats/blob/master/src/main/resources/avro/bdg.avdl#L547
[adam]: https://github.com/bigdatagenomics/adam
[avocado]: https://github.com/bigdatagenomics/avocado
[code docs]: http://www.hammerlab.org/guacamole/docs/#org.hammerlab.guacamole.package
[maven]: http://maven.apache.org/
