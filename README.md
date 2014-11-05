guacamole
=========
[![Build Status](https://travis-ci.org/hammerlab/guacamole.svg?branch=master)](https://travis-ci.org/hammerlab/guacamole)
Guacamole is a framework for identifying mutations from Next Generation
Sequencing data, a procedure known as variant calling. Guacamole currently
includes a demonstration germline (non-cancer) variant caller as well as a
somatic variant caller for finding mutations present in a cancer. More
development effort has gone into the somatic caller so far.

The emphasis is on a readable codebase that can be readily understood and
adapted for experimentation.

Guacamole is written in Scala using the Apache Spark engine for distributed
processing. It can run on a single computer or on a Hadoop cluster. Guacamole
reads BAM files and generates VCFs.

Guacamole uses many ideas and some functionality from
[ADAM](https://github.com/bigdatagenomics/adam). We've also taken inspiration
from the [Avocado](https://github.com/bigdatagenomics/avocado) project.

For hacking Guacamole, see our [code docs](http://blog.hammerlab.org/guacamole/docs/#package).

## Running Guacamole on a Single Node

Build:

```
mvn package
```

Run:

```
scripts/guacamole threshold \
	-reads src/test/resources/chrM.sorted.bam \
	-out /tmp/OUT.vcf
```

Try 
```
scripts/guacamole -h
```
for a list of implemented variant callers, or

```
scripts/guacamole <caller> -h
```
for help on a particular variant caller.

# License

Guacamole is released under an [Apache 2.0 license](LICENSE.txt).

YourKit is kindly supporting this open source project with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
[Java](http://www.yourkit.com/java/profiler/index.jsp) and [.NET](http://www.yourkit.com/.net/profiler/index.jsp) applications.


