/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.commands

import java.io.InputStreamReader
import java.text.{ DecimalFormatSymbols, DecimalFormat }
import java.util.Locale
import javax.script._

import com.google.common.io.CharStreams
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.Common.Arguments.NoSequenceDictionary
import org.hammerlab.guacamole.pileup._
import org.hammerlab.guacamole.reads._
import org.hammerlab.guacamole._
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

/**
 * This is an experimental command that runs user-specified javascript over reads or pileups.
 *
 * An invocation on the commandline looks like this:
 *
 * guacamole eval /path/to/my/reads.bam :: "pileup().depth()"
 *
 * The first positional arguments give paths to BAMs (any number of BAMs may be analyzed simultaneously), followed by
 * a double colon (::) to indicate the end of the list of input files. The remaining positional arguments give javascript
 * expressions to evaluate.
 *
 * The results of evaluating each expression on each input item are converted to strings and written as csv to the
 * file given by the --out argument or stdout.
 *
 * If any expression evaluates to null, the input item is skipped. For example:
 *
 * guacamole eval /path/to/my/reads.bam :: contig locus "if (ref == 'A') pileup().depth(); else null"
 *
 * will output a csv with with lines for every locus where the reference base is "A". Each line will give the contig,
 * locus, and depth.
 *
 * Several "domains" are supported, including "pileups" and "reads". The default domain is "pileups," which runs each of
 * the expressions at every genomic locus (or the loci specified by --loci). The "reads" domain runs the expressions on
 * each read individually. For example:
 *
 * guacamole eval src/test/resources/synth1.tumor.100k-200k.withmd.bam ::  \
 *    "if (read.isMateMapped() && read.inferredInsertSize() > 5000) read.inferredInsertSize()" \
 *    --domain reads
 *
 * Will output reads inferred insert sizes when they are > 5000.
 *
 * For complex scripts, put your functions in a separate file and include it with --include-file. Then just invoke your
 * functions on the commandline.
 *
 * LABELS
 *
 * Both the input files and the expressions to evaluate can be associated with labels by preceeding them with an
 * argument that ends with a colon:
 *
 * guacamole eval normal: /path/to/my/reads.bam tumor: /path/to/my/reads2.bam :: depth: "pileup().depth()"
 *
 * Labeling input files allows javascript code to refer to them by name. Expression labels are used in the header line
 * in the output csv (currently headers are written only for results written to stdout).
 *
 * EVALUATION CONTEXT: PILEUPS DOMAIN
 *
 * In the "pileups" domain, there is a global JS function "pileup()" that gives the pileup for an input file at the current
 * locus. If an argument is given it can be either an int i to read the pileup for the i'th file, with "pileup(0)" giving
 * the first pileup, or a string giving an input file label. If no argument is given it defaults to 0. See the
 * `Evaluation.ScriptPileup` class for the methods available on a pileup.
 *
 * Global variables available in the pileups domain:
 *  contig  the chromosome (string)
 *  locus   the genomic locus (int)
 *  ref     the reference base at this locus (string)
 *
 * EVALUATION CONTEXT: READS DOMAIN
 *
 * Global variables:
 *  read  the read. See the `ScriptRead` class for docs.
 *  path  the path to the BAM file where this read came from (string).
 *  label the label of the BAM file where this read came from. Defaults to path if there is no label. (string)
 *
 * IMPLEMENTATION NOTES
 *
 * Javascript was chosen because it ships with the JVM. Since we use only the public javax.script API, it may be possible
 * to extend this to work with other languages. We're aiming for compatability with both the Java 7 (rhino) and Java 8
 * (nashorn) implementations.
 *
 * The commandline arguments are somewhat awkward since args4j is very limited. For example, it can't handle options that
 * take variable numbers of positional arguments and respect shell quoting. If we switch to a different argument
 * parsing package this may get better.
 *
 */
object EvalCommand {

  protected class Arguments extends DistributedUtil.Arguments with NoSequenceDictionary {

    @Argument(required = true, multiValued = true,
      usage = "[label1: ]bam1 ... :: [header1: ]expression1 ... ")
    var fields: Array[String] = Array.empty

    @Args4jOption(name = "--out", usage = "Output path. Default: stdout")
    var out: String = ""

    @Args4jOption(name = "--include-empty-pileups",
      usage = "Include empty pileups. Only meaningful in the 'pilups' domain.")
    var includeEmptyPileups: Boolean = false

    @Args4jOption(name = "--separator", usage = "CSV field separator")
    var separator: String = ", "

    @Args4jOption(name = "--domain",
      usage = "Input to evaluate, either: 'pileups' (default), 'reads', 'mapped-reads', or 'unmapped-reads'")
    var domain: String = "pileups"

    @Args4jOption(name = "--include-file", usage = "Code files to include", handler = classOf[StringArrayOptionHandler])
    var includeFiles: ArrayBuffer[String] = ArrayBuffer.empty

    var includeCode = new ArrayBuffer[String]()
    @Args4jOption(name = "--include-code", metaVar = "CODE", usage = "Code to include")
    def addCode(arg: String): Unit = includeCode += arg

    var codeArgs = new ArrayBuffer[String]()
    @Args4jOption(name = "--arg", metaVar = "NAME=VALUE",
      usage = "Argument for script, given as NAME=VALUE")
    def codeArgs(arg: String): Unit = codeArgs += arg

    @Args4jOption(name = "-q", usage = "Quiet: less stdout")
    var quiet: Boolean = false
  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "eval"
    override val description = "run arbitrary javascript over reads or pileups"

    /**
     * Parse positional arguments of the form:
     *  [label:] bam ... :: [label:] expression ...
     *
     * @param fields
     * @return (bam labels, bams, expression labels, expressions)
     */
    def parseFields(fields: Seq[String]): (Seq[String], Seq[String], Seq[String], Seq[String]) = {
      val bams = ArrayBuffer.newBuilder[String]
      val bamLabels = ArrayBuffer.newBuilder[String]
      val expressions = ArrayBuffer.newBuilder[String]
      val expressionLabels = ArrayBuffer.newBuilder[String]
      var label = ""
      var parsingExpressions = false
      var nextDefaultNum = 1
      fields.foreach(fieldRaw => {
        val field = fieldRaw.trim
        if (field == "::") {
          if (parsingExpressions) {
            throw new RuntimeException("Couldn't parse fields: multiple separator (=>) arguments")
          } else if (label.nonEmpty) {
            throw new RuntimeException("Couldn't parse fields: no value for label: " + label)
          } else {
            parsingExpressions = true
          }
        } else {
          if (label.nonEmpty) {
            if (parsingExpressions) {
              expressions += fieldRaw
              expressionLabels += label
            } else {
              bams += fieldRaw
              bamLabels += label
            }
            label = ""
          } else {
            if (field.endsWith(":")) {
              label = field.dropRight(1)
            } else {
              if (parsingExpressions) {
                expressions += field
                expressionLabels += field.replaceAll("\n", " ").substring(0, Math.min(field.length, 20))
              } else {
                bams += field
                bamLabels += "r%d".format(nextDefaultNum)
                nextDefaultNum += 1
              }
            }
          }
        }
      })
      (bamLabels.result, bams.result, expressionLabels.result, expressions.result)
    }

    /**
     * Files that are automatically included as (description, path) pairs.
     */
    val standardIncludePaths = Seq(
      ("Preamble", "EvalCommand/preamble.js"))

    val standardIncludes = standardIncludePaths.map({
      case (name, path) => {
        val stream = EvalCommand.getClass.getClassLoader.getResourceAsStream(path)
        assert(stream != null, "Couldn't load: " + path)
        (name, CharStreams.toString(new InputStreamReader(stream)))
      }
    })

    /**
     * Given a list of strings of the form "A=B", return a fragment of javascript that creates an object called "args"
     * with the specified properties.
     */
    def buildArgsCode(codeArgs: Seq[String]): String = {
      val syntax = """^([A-z_][A-z0-9_]+)=(.+)""".r
      val nameValues = codeArgs.map(s => s match {
        case syntax(name: String, value: String) => (name, value)
        case _                                   => throw new IllegalArgumentException("Couldn't parse script arg: " + s)
      })
      "args = {\n" + nameValues.map(pair => "    '%s': '%s'".format(pair._1, pair._2)).mkString(",\n") + "\n}"
    }

    /**
     * Return a list of (description, code fragment) giving the code fragments to run, based on user arguments.
     */
    def makeScriptFragments(args: Arguments): Seq[(String, String)] = {
      lazy val filesystem = FileSystem.get(new Configuration())
      val argsFragment = Seq(("Arguments", buildArgsCode(args.codeArgs)))
      val extraIncludesFromFiles = args.includeFiles.map(path => {
        val code = IOUtils.toString(new InputStreamReader(filesystem.open(new Path(path))))
        ("Code from: %s".format(path), code)
      })
      val extraIncludesFromCommandline = args.includeCode.zipWithIndex.map(pair => {
        ("Code block %d".format(pair._2), pair._1)
      })
      standardIncludes ++ argsFragment ++ extraIncludesFromFiles ++ extraIncludesFromCommandline
    }

    /**
     * Evaluate a sequence of code fragments in the context given by bindings.
     */
    def runScriptFragments(fragments: Seq[String], bindings: Bindings): Unit = {
      fragments.foreach(fragment => {
        Evaluation.eval(fragment, Evaluation.compilingEngine.compile(fragment), bindings)
      })
    }

    override def run(args: Arguments, sc: SparkContext): Any = {
      val (bamLabels, bams, expressionLabels, expressions) = parseFields(args.fields)
      assert(bamLabels.size == bams.size)
      assert(expressionLabels.size == expressions.size)

      val namedScriptFragments = makeScriptFragments(args)
      val scriptFragments = namedScriptFragments.map(_._2)

      if (!args.quiet) {
        println("Read inputs (%d):".format(bams.size))
        bams.zip(bamLabels).foreach(pair => println("%20s : %s".format(pair._2, pair._1)))
        println()
        println("Script fragments:")
        namedScriptFragments.foreach({
          case (name, fragment) => {
            println(name)
            if (fragment.size < 5000) {
              println(Evaluation.stringWithLineNumbers(fragment))
            } else {
              println("(skipped due to size)")
            }
            println()
          }
        })
        println()
        println("Expressions (%d):".format(expressions.size))
        expressionLabels.zip(expressions).foreach({
          case (label, expression) =>
            println("%20s : %s".format(label, expression))
        })
        println()

      }

      // Test that our script runs and that and expressions at least compile, so we error out immediately if not.
      val bindings = new SimpleBindings()
      expressions.foreach(Evaluation.compilingEngine.compile _)

      val readSets = bams.zipWithIndex.map({
        case (path, index) => ReadSet(
          sc, path, false, Read.InputFilters.empty, token = index, contigLengthsFromDictionary = !args.noSequenceDictionary)
      })

      assert(readSets.forall(_.sequenceDictionary == readSets(0).sequenceDictionary),
        "Samples have different sequence dictionaries: %s."
          .format(readSets.map(_.sequenceDictionary.toString).mkString("\n")))

      val separator = args.separator
      val totalRows = sc.accumulator(0L)

      val result: RDD[String] = if (args.domain == "pileups") {
        // Running over pileups.

        val loci = Common.loci(args, readSets(0))
        val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, readSets.map(_.mappedReads): _*)

        DistributedUtil.pileupFlatMapMultipleRDDsWithState[String, Option[(Seq[CompiledScript], Bindings)]](
          readSets.map(_.mappedReads),
          lociPartitions,
          !args.includeEmptyPileups, // skip empty
          initialState = None,
          (maybeState, pileups) => {
            val (compiledExpressions, bindings) = maybeState match {
              case None => {
                val newBindings = new SimpleBindings()
                runScriptFragments(scriptFragments, newBindings)
                (expressions.map(Evaluation.compilingEngine.compile _), newBindings)
              }
              case Some(pair) => pair
            }

            // Update bindings for the current locus.
            val scriptPileups = pileups.map(new Evaluation.ScriptPileup(_))
            bindings.put("ref", Bases.baseToString(pileups(0).referenceBase))
            bindings.put("locus", pileups(0).locus)
            bindings.put("contig", pileups(0).referenceName)
            bindings.put("_pileups", scriptPileups)
            bindings.put("_by_label", bamLabels.zip(scriptPileups).toMap)

            val evaluatedExpressionResults = expressions.zip(compiledExpressions).map(
              pair => Evaluation.eval(pair._1, pair._2, bindings))

            val result = if (evaluatedExpressionResults.exists(_ == null)) {
              Iterator.empty
            } else {
              totalRows += 1
              Iterator.apply(evaluatedExpressionResults.map(Evaluation.jsValueToString(_)).mkString(separator))
            }
            (Some((compiledExpressions, bindings)), result)
          })
      } else if (args.domain == "reads" || args.domain == "mapped-reads" || args.domain == "unmapped-reads") {
        // Running over reads.
        val unionAll = sc.union(readSets.map(_.reads))
        val union = args.domain match {
          case "reads"          => unionAll
          case "mapped-reads"   => unionAll.filter(_.isMapped)
          case "unmapped-reads" => unionAll.filter(!_.isMapped)
        }

        union.mapPartitions(reads => {
          val bindings = new SimpleBindings()
          runScriptFragments(scriptFragments, bindings)

          val compiledExpressions = expressions.map(Evaluation.compilingEngine.compile _)

          reads.flatMap(read => {
            bindings.put("read", new Evaluation.ScriptRead(read))
            bindings.put("label", bamLabels(read.token))
            bindings.put("path", bams(read.token))

            val evaluatedExpressionResults = expressions.zip(compiledExpressions).map(
              pair => Evaluation.eval(pair._1, pair._2, bindings))

            if (evaluatedExpressionResults.exists(_ == null)) {
              Iterator.empty
            } else {
              totalRows += 1
              Iterator.apply(evaluatedExpressionResults.map(Evaluation.jsValueToString(_)).mkString(separator))
            }
          })
        })
      } else {
        throw new IllegalArgumentException("Unsupported domain: %s".format(args.domain))
      }

      if (args.out == "none") {
        // No output (this is used in unit tests). We collect the result to force the computation.
        result.collect()

      } else if (args.out.nonEmpty) {
        // Write using hadoop.
        result.saveAsTextFile(args.out)

      } else {
        // Write to stdout.
        println("BEGIN RESULT")
        println("*" * 60)
        println(expressionLabels.mkString(separator))

        result.collect.foreach(println _)
        println("*" * 60)
        println("END RESULT")
      }
      println("Generated %,d rows.".format(totalRows.value))

      DelayedMessages.default.print()

      result // Unit tests use this return value.
    }
  }
}

/**
 * Helper functions and wrapper classes for evaluating javascript over pileups and reads.
 *
 * Although javascript can call arbitrary methods on any JVM object and would work fine with our usual Guacamole classes
 * like Pileup, we give a more convenient scripting interface by wrapping the most common classes.
 *
 */
object Evaluation {
  val factory = new javax.script.ScriptEngineManager
  val engine = factory.getEngineByName("JavaScript")
  val compilingEngine = engine.asInstanceOf[Compilable]

  assert(engine != null)
  assert(compilingEngine != null)

  private val decimalFormatter = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))
  decimalFormatter.setMaximumFractionDigits(10)

  /**
   * Convert a javascript value to a string for csv output. Since javascript has no int type, we special case doubles
   * here to output them without decimals if they are in fact integers.
   * @param value arbitrary javascript value
   * @return string representation of that value
   */
  def jsValueToString(value: Any): String = value match {
    case v: Double => decimalFormatter.format(v)
    case other     => StringEscapeUtils.escapeCsv(value.toString)
  }

  /**
   * Evaluate the given compiled javascript code with the specified bindings.
   *
   * For efficiency, this takes pre compiled code. The code argument should be the source code that was used to generate
   * it, and is used to display a meaningful message if an error occurs.
   *
   * @param code source code used to generate compiledCode
   * @param compiledCode compiled code
   * @param bindings context to run in
   * @return the result of the evaluation
   */
  def eval(code: String, compiledCode: CompiledScript, bindings: Bindings): Any = {
    try {
      compiledCode.eval(bindings)
    } catch {
      case e: javax.script.ScriptException => {
        throw new javax.script.ScriptException(e.getMessage
          + " while evaluating:\n" + stringWithLineNumbers(code) + "\nwith these bindings:\n\t"
          + JavaConversions.asScalaSet(
            bindings.keySet).map(key => "%20s = %20s".format(key, bindings.get(key))).mkString("\n\t"),
          e.getFileName,
          e.getLineNumber,
          e.getColumnNumber)
      }
    }
  }

  def stringWithLineNumbers(s: String): String = {
    s.split("\n").zipWithIndex.map(pair => "\t%3d) %s".format(pair._2 + 1, pair._1)).mkString("\n")
  }

  /**
   * Wrapper for a PileupElement.
   */
  class ScriptPileupElement(val wrapped: PileupElement) {
    lazy val isMatch = wrapped.isMatch
    lazy val isMismatch = wrapped.isMismatch
    lazy val isInsertion = wrapped.isInsertion
    lazy val isDeletion = wrapped.isDeletion
    lazy val isMidDeletion = wrapped.isMidDeletion

    lazy val read = new ScriptRead(wrapped.read)
    lazy val sequence = Bases.basesToString(wrapped.sequencedBases)
    lazy val reference = Bases.basesToString(wrapped.referenceBases)
    lazy val baseQuality = wrapped.qualityScore
    lazy val readPosition = wrapped.readPosition
  }

  /** Wrapper for Pileup */
  class ScriptPileup(wrapped: Pileup) {
    private lazy val byAllele: Map[String, Seq[ScriptPileupElement]] =
      elements.groupBy(_.sequence).withDefaultValue(Seq.empty)

    lazy val referenceName = wrapped.referenceName
    lazy val contig = referenceName
    lazy val reference: String = Bases.baseToString(wrapped.referenceBase)

    lazy val locus = wrapped.locus
    lazy val depth = wrapped.depth
    lazy val elements = wrapped.elements.filter(_.alignment != Clipped).map(new ScriptPileupElement(_))
    lazy val elementsIncludingClipped = wrapped.elements.map(new ScriptPileupElement(_))

    /** Alleles sequenced at this locus. */
    lazy val alleles: Seq[String] =
      Seq(reference) ++
        byAllele.filterKeys(_ != reference).toSeq.sortBy(-1 * _._2.count(_.wrapped.alignment != Clipped)).map(_._1)

    /** Return a pileup containing the subset of the current pileup whose reads match the given allele. */
    def withAllele(allele: String): ScriptPileup = {
      val elements = byAllele(allele).map(_.wrapped)
      new ScriptPileup(new Pileup(referenceName, locus, wrapped.referenceBase, elements))
    }

    lazy val numMatch = wrapped.elements.count(_.isMatch)
    lazy val numMismatch = wrapped.elements.count(_.isMismatch)
    lazy val numInsertion = wrapped.elements.count(_.isInsertion)
    lazy val numDeletion = wrapped.elements.count(_.isDeletion)
    lazy val numMidDeletion = wrapped.elements.count(_.isMidDeletion)
    lazy val numDeleted = wrapped.elements.count(e => e.isDeletion || e.isMidDeletion)

    /** Most common allele sequenced at this locus. */
    lazy val topAllele = byAllele.toSeq.sortBy(-1 * _._2.length).headOption.map(_._1).getOrElse("none")

    /** Most common non-reference allele, or the string "none" if there are no non-reference alleles sequenced. */
    lazy val topVariantAllele: String = if (alleles.size > 1) alleles(1) else "none"

    lazy val numNotMatch = depth - numMatch
    lazy val numTopVariantAllele = elements.count(_.sequence == topVariantAllele)
    lazy val numOtherAllele = depth - numMatch - numTopVariantAllele

    override def toString() = "<Pileup at %s:%d of %d elements>".format(referenceName, locus, elements.size)

    /** Samtools-style mpileup representation of a pileup. */
    def mpileup(): String = mpileup(true)
    def mpileupNoMappingQualities(): String = mpileup(false)
    def mpileup(includeMappingQualities: Boolean): String = {
      def indicateStrand(value: String, isPositiveStrand: Boolean): String = value match {
        case "." if (!isPositiveStrand) => ","
        case _ if (isPositiveStrand)    => value.toUpperCase
        case _                          => value.toLowerCase
      }

      def encodeQuality(quality: Int): Char = (quality + 33).toChar

      val result = StringBuilder.newBuilder
      result ++= "%s\t%d\t%s\t%d ".format(referenceName, locus + 1, reference, depth)

      // Note: we are ignoring clipped bases and bases inside introns ('N' cigar operator). This is different than
      // samtools, which outputs '>' and '<' in this case.

      // Bases
      result ++= elements.map(element => indicateStrand(element.wrapped.alignment match {
        case Match(_, _)                 => "."
        case Mismatch(base, _, _)        => Bases.baseToString(base)
        case Insertion(bases, _)         => "+%d%s".format(bases.length, Bases.basesToString(bases))
        case Deletion(referenceBases, _) => "-%d%s".format(referenceBases.length, Bases.basesToString(referenceBases))
        case MidDeletion(_)              => "*"
        case _                           => ""
      }, element.read.isPostiveStrand)).mkString
      result += '\t'

      // Base qualitites
      val qualities = elements.flatMap(element => element.wrapped.alignment match {
        case MatchOrMisMatch(_, quality) => Seq(quality)
        case Insertion(_, qualities)     => qualities
        case _                           => Seq.empty
      }).map(_.toInt)
      result ++= qualities.map(encodeQuality _)

      // Mapping qualities
      if (includeMappingQualities) {
        result += '\t'
        result ++= elements.map(element => encodeQuality(element.read.alignmentQuality))
      }

      result.result
    }
  }

  /** Wrapper for Read, MappedRead, etc. */
  class ScriptRead(wrapped: Read) {
    lazy val token = wrapped.token
    lazy val sequence = Bases.basesToString(wrapped.sequence)
    lazy val baseQualities = wrapped.baseQualities.map(_.toInt)
    lazy val isDuplicate = wrapped.isDuplicate
    lazy val sampleName = wrapped.sampleName
    lazy val isMapped = wrapped.isMapped

    // Mapped read properties
    private lazy val mappedRead: MappedRead = wrapped match {
      case r: MappedRead                  => r
      case r: PairedRead[_] if r.isMapped => r.read.asInstanceOf[MappedRead]
      case _                              => throw new IncompatibleClassChangeError("Not a mapped read: " + toString)
    }
    lazy val contig = mappedRead.referenceContig
    lazy val referenceContig = contig
    lazy val start = mappedRead.start
    lazy val end = mappedRead.end
    lazy val alignmentQuality = mappedRead.alignmentQuality
    lazy val cigarString = mappedRead.cigar.toString
    lazy val isPostiveStrand = mappedRead.isPositiveStrand

    // Paired read properties
    private lazy val pairedRead: PairedRead[_] = wrapped match {
      case r: PairedRead[_] => r
      case _                => throw new IncompatibleClassChangeError(("Not a paired read: " + toString))
    }
    lazy val isFirstInPair = pairedRead.isFirstInPair
    lazy val isMateMapped = pairedRead.isMateMapped
    lazy val mateStart = pairedRead.mateAlignmentProperties.get.start
    lazy val mateReferenceContig = pairedRead.mateAlignmentProperties.get.referenceContig
    lazy val mateContig = mateReferenceContig
    lazy val mateIsPositiveStrand = pairedRead.mateAlignmentProperties.get.isPositiveStrand
    lazy val inferredInsertSize = pairedRead.mateAlignmentProperties.get.inferredInsertSize.getOrElse(-1)

    // Extra properties
    def reverseComplementBases: String = {
      Bases.basesToString(Bases.reverseComplement(wrapped.sequence))
    }
  }
}
