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

You'll need Guacamole and its dependencies on your classpath to run, which you can get in a few ways, described below; all of them are compatible with the options for running Guacamole detailed in the [Running](#running) section.

### Guacamole JAR + Dependencies File
The simplest (and fastest) option builds a Guacamole JAR as well as a file with paths to dependencies' JARs (in your local Maven cache):

```bash
mvn package -DskipTests
```

The JAR and dependencies file will be at `target/guacamole-0.0.1-SNAPSHOT.jar` and `target/dependencies`, respectively.

This uses the `guac` Maven profile, which is active by default, and so is equivalent to:

```bash
mvn package -DskipTests -Pguac
```

### Assembly JAR
Another option is to build one assembly ("uber") JAR containing Guacamole and all its dependencies:

```bash
mvn package -DskipTests -Puber
```

This has the advantage that there is just one file to ship around, e.g. to a remote server you may want to run on, but copying all the dependency-JARs (62MB total at last count) into one can add a minute or so to the run-time.

### Guacamole + Dependencies JARs
Finally, you can build the Guacamole JAR as well as a "dependencies JAR"; the latter is like the aforementioned assembly JAR, but without any of the guacamole classes:

```bash
# Build a shaded JAR with all of Guacamole's transitive dependencies (excepting the two aforementioned shaded ones).
mvn package -DskipTests -Pdeps

# Build both "guac" and "deps" JARs at once.
mvn package -DskipTests -Pguac,deps
```

This approach combines some advantages of the previous two: the Guacamole JAR is small and can be built and rebuilt quickly (e.g. while developing), but there are just two files that need be copied in order to run in any remote environment.

## Running

### `scripts/guacamole`
Having built Guacamole in any of the ways described above, you can run it using [`scripts/guacamole`](scripts/guacamole), a thin wrapper around `spark-submit` that makes it easy to run locally or on a cluster:

```bash
scripts/guacamole somatic-joint \
    src/test/resources/synth1.normal.100k-200k.withmd.bam \
    src/test/resources/synth1.tumor.100k-200k.withmd.bam \
    --reference /path/to/your/b37.fasta \
    --out /tmp/out.vcf 
```

This calls germline and somatic variants from some small test BAMs using the
*somatic-joint* variant caller, which works with any number of tumor/normal
samples from the same patient. We've observed it to run in around 2 minutes on a 3.1 GHz,
16gb memory Macbook.

#### Usage
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

This variable is interpreted as a comma-separated list of [Spark properties files](http://spark.apache.org/docs/1.6.1/configuration.html#dynamically-loading-spark-properties), which are taken to be relative to the current directory, of the [`conf/`](conf) directory if a path is not found in the former.

In all cases, it will be seeded with the contents of [conf/kryo](conf/kryo):

```
spark.serializer          org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator    org.hammerlab.guacamole.kryo.Registrar
spark.kryo.registrationRequired   true
spark.kryoserializer.buffer       4mb
```

Additionally, if `$GUAC_SPARK_CONFS` is not set, [conf/local](conf/local) will be used:

```
spark.master  local[1]
spark.driver.memory 4g
```

This configures a local Spark run using 1 executor/core and 4GB of memory.

[The conf/ directory](conf/) contains further example configuration blocks that can be mixed in; see below for more examples. Files in [`conf/`](conf) are `.gitignore`d by default, so feel free to keep your own custom configs there.

### Running on a Hadoop Cluster
Guacamole currently builds against Spark 1.6.1 and Hadoop 2.7.0, though it will likely run fine with versions close to those.

For example, you could run with [cluster-](conf/cluster) and [GC-logging](conf/gc) configs from the [conf/](conf/) directory like so:

```bash
export GUAC_SPARK_CONFS=cluster,gc
scripts/guacamole \
  somatic-standard \
    --normal-reads /hdfs/path/to/normal.bam \
    --tumor-reads /hdfs/path/to/tumor.bam \
    --reference /local/path/to/reference.fasta \
    --out /tmp/out.vcf
```

Note that, even when running with HDFS as the default filesystem, `--reference` must be a local path.

### `scripts/guacamole-shell`
The [`scripts/guacamole-shell`](scripts/guacamole-shell) command wraps `spark-shell`, putting Guacamole and all its dependencies on the classpath, and parsing Spark configuration from the `$GUAC_SPARK_CONFS` arg as described above.

```
$ GUAC_SPARK_CONFS=local-multi,driver-10g scripts/guacamole-shell
Using Spark properties files: local-multi,driver-10g
Spark configs:
	spark.serializer                  org.apache.spark.serializer.KryoSerializer
	spark.kryo.registrator            org.hammerlab.guacamole.kryo.Registrar
	spark.kryo.registrationRequired   true
	spark.kryoserializer.buffer       4mb
	spark.master    local[*]
	spark.driver.memory 10g
2016-09-23 02:41:27 WARN  NativeCodeLoader:62 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.1
      /_/
…
scala> :paste
// Entering paste mode (ctrl-D to finish)

import org.hammerlab.guacamole.commands.SomaticStandard.{Arguments, Caller}

val args = new Arguments
args.normalReads = "src/test/resources/cancer-wgs1/normal.tiny.bam"
args.tumorReads = "src/test/resources/cancer-wgs1/recurrence.tiny.bam"
args.referencePath = "/path/to/chr12.fasta"
args.loci = "chr12"

val variants = Caller.computeVariants(args, sc)._1
vs.count
// Exiting paste mode, now interpreting.
…
res0: Long = 2
```

Both `scripts/guacamole-shell` and `scripts/guacamole` will work if any of the "[Building](#building)" options above have been executed.

#### Running on a Java 7 Cluster
Guacamole requires Java 8 to build and run. If running on a Hadoop cluster running Java 7, you'll want to point your driver and executors at a `$JAVA_HOME` where Java 8 is installed, locally on all nodes:

```bash
cat <<EOF >conf/j8
spark.executorEnv.JAVA_HOME       /path/to/java8
spark.driverEnv.JAVA_HOME         /path/to/java8
EOF
GUAC_SPARK_CONFS=cluster,j8 scripts/guacamole …
```

# Running the test suite
The full test suite can be run with:

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
