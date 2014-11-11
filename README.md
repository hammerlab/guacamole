guacamole
=========
[![Build Status](https://travis-ci.org/hammerlab/guacamole.svg?branch=master)](https://travis-ci.org/hammerlab/guacamole)
Guacamole is a Spark-based variant calling framework inspired by Avocado.

The goal is a small, readable codebase that can be understood quickly and adapted for experimentation.

Currently, the variant callers included are toy implementations for understanding Spark performance, and are not suitable
for production use.


## Running Guacamole

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
![image](https://cloud.githubusercontent.com/assets/455755/4988560/97757f12-6935-11e4-9270-f5fc42f9b585.png)
