guacamole
=========
[![Build Status](https://travis-ci.org/hammerlab/guacamole.svg?branch=master)](https://travis-ci.org/hammerlab/guacamole)
[![Coverage Status](https://coveralls.io/repos/github/hammerlab/guacamole/badge.svg?branch=master)](https://coveralls.io/github/hammerlab/guacamole?branch=master)

Guacamole is a framework for variant calling, i.e. identifying DNA mutations
from [Next Generation Sequencing][seq] data. It currently includes a toy
germline (non-cancer) variant caller as well as a somatic variant caller for
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

Guacamole uses ideas and some functionality from [ADAM][]. It also takes
inspiration from the [Avocado][] project.

For hacking Guacamole, see our [code docs][].

## Building

Building Guacamole requires [Apache Maven][maven] (version 3.0.4 or higher).

You'll generally want two JARs to be on your classpath when you run Guacamole:

- the main Guacamole JAR, and
- a shaded JAR with all of Guacamole's transitive dependencies.

The former can be built in any of the following ways:

```
mvn package
mvn package -DskipTests  # don't run tests, just package the JAR.
mvn package -Pguac       # this is the default Maven profile, but can be useful for combining with other profiles as examples below demonstrate.
```

The latter can be built using the `deps` profile:

```
mvn package -Pdeps
mvn package -Pdeps -DskipTests
```

You can also build both at once:

```
mvn package -Pguac,deps
mvn package -Pguac,deps -DskipTests
```

## Running

### Running Locally
`scripts/guacamole` makes it easy to run locally:

```
scripts/guacamole somatic-joint \
    src/test/resources/synth1.normal.100k-200k.withmd.bam \
    src/test/resources/synth1.tumor.100k-200k.withmd.bam \
    --reference-fasta /path/to/your/b37.fasta \
    --out /tmp/out.vcf 
```

Change the reference fasta above to point to a local copy of the B37 human
reference.

This calls germline and somatic variants from some small test BAMs using the
*joint-caller* variant caller, which works with any number of tumor/normal
samples from the same patient. It takes around 2 minutes to run on my 3.1 GHz,
16gb memory Macbook.

Try 
```
scripts/guacamole -h
```
for a list of implemented variant callers, or

```
scripts/guacamole <caller> -h
```
for help on a particular variant caller.

### Running on a Hadoop Cluster

Guacamole currently builds against Spark 1.6.1 and Hadoop 2.7.0, though it will likely run fine with versions close to those.

Here is an example command to get started using Guacamole in Spark's yarn
cluster mode. You'll probably have to modify it for your environment. 

```
version=0.0.1-SNAPSHOT
spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--executor-memory 4g \
	--driver-memory 10g \
	--num-executors 1000 \
	--executor-cores 1 \
	--class org.hammerlab.guacamole.Main \
	--jars target/guacamole-deps-only-$version.jar \
	--verbose \
	target/guacamole-$version.jar \
	somatic-joint \
    		/hdfs/path/to/normal.bam \
    		/hdfs/path/to/tumor.bam \
    		--reference-fasta /local/path/to/reference.fasta \
    		--out /tmp/out.vcf 
```

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
