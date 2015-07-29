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
import javax.script._

import com.google.common.io.CharStreams
import htsjdk.samtools.Cigar
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.Common.Arguments.{ NoSequenceDictionary }
import org.hammerlab.guacamole.commands.Evaluation.{ ScriptPileup }
import org.hammerlab.guacamole.pileup.{ Pileup }
import org.hammerlab.guacamole.reads._
import org.hammerlab.guacamole._
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

object EvalCommand {

  protected class Arguments extends DistributedUtil.Arguments with NoSequenceDictionary {

    @Argument(required = true, multiValued = true,
      usage = "[label1: ]bam1 ... - [header1: ]expression1 ... ")
    var fields: Array[String] = Array.empty

    @Args4jOption(name = "--out",
      usage = "Output path. Default: stdout")
    var out: String = ""

    @Args4jOption(name = "--include-empty",
      usage = "Include empty pileups.")
    var includeEmpty: Boolean = false

    @Args4jOption(name = "--separator",
      usage = "CSV field separator")
    var separator: String = ", "

    @Args4jOption(name = "--domain",
      usage = "Input to evaluate, either: 'pileups' (default) or 'reads'")
    var domain: String = "pileups"

    @Args4jOption(name = "--include-file", usage = "Code files to include", handler = classOf[StringArrayOptionHandler])
    var includeFiles: ArrayBuffer[String] = ArrayBuffer.empty

    var includeCode = new ArrayBuffer[String]()
    @Args4jOption(name = "--include-code",
      usage = "Code to include")
    def addCode(arg: String): Unit = includeCode += arg

    @Args4jOption(name = "-q", usage = "Quiet: less stdout")
    var quiet: Boolean = false
  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "eval"
    override val description = "run javascript code over pileups"

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

    val standardIncludePaths = Seq(
      ("Preamble", "EvalCommand/preamble.js"))

    val standardIncludes = standardIncludePaths.map({case (name, path) => {
        val stream = ClassLoader.getSystemClassLoader.getResourceAsStream(path)
        assert(stream != null, "Couldn't load: " + path)
      (name, CharStreams.toString(new InputStreamReader(stream)))
    }})

    def makeScriptFragments(args: Arguments): Seq[(String, String)] = {
      lazy val filesystem = FileSystem.get(new Configuration())
      val extraIncludesFromFiles = args.includeFiles.map(path => {
        val code = IOUtils.toString(new InputStreamReader(filesystem.open(new Path(path))))
        ("Code from: %s".format(path), code)
      })
      val extraIncludesFromCommandline = args.includeCode.zipWithIndex.map(pair => {
        ("Code block %d".format(pair._2), pair._1)
      })
      standardIncludes ++ extraIncludesFromFiles ++ extraIncludesFromCommandline
    }

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
            if (fragment.size < 1000) {
              println(Evaluation.stringWithLineNumbers(fragment))
            } else {
              println("(skipped due to size)")
            }
            println()
          }
        })
        println()
        println("Expressions (%d):".format(expressions.size))
        expressions.zip(expressionLabels).foreach({
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

      val result: RDD[String] = if (args.domain == "pileups") {
        // Running over pileups.

        val loci = Common.loci(args, readSets(0))
        val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, readSets.map(_.mappedReads): _*)

        DistributedUtil.pileupFlatMapMultipleRDDsWithState[String, Option[(Seq[CompiledScript], Bindings)]](
          readSets.map(_.mappedReads),
          lociPartitions,
          !args.includeEmpty, // skip empty
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
            val scriptPileups = pileups.map(new ScriptPileup(_))
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
              Iterator.apply(evaluatedExpressionResults.map(Evaluation.toJSON(_)).mkString(separator))
            }
            (Some((compiledExpressions, bindings)), result)
          })
      } else if (args.domain == "reads") {
        // Running over reads.
        val union = sc.union(readSets.map(_.reads))
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
              Iterator.apply(evaluatedExpressionResults.map(Evaluation.toJSON _).mkString(separator))
            }
          })
        })
      } else {
        throw new IllegalArgumentException("Unsupported domain: %s".format(args.domain))
      }

      if (args.out == "none") {
        // No output.

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

      DelayedMessages.default.print()

      result // Unit tests use this.
    }
  }
}
object Evaluation {
  val factory = new javax.script.ScriptEngineManager
  val engine = factory.getEngineByName("JavaScript")
  val compilingEngine = engine.asInstanceOf[Compilable]
  val invocableEngine = engine.asInstanceOf[Invocable]

  val jsonObject = try {
    Some(engine.eval("JSON"))
  } catch {
    case _: Exception => None
  }

  assert(engine != null)
  assert(compilingEngine != null)
  assert(invocableEngine != null)

  def toJSON(jsValue: Any): String = jsonObject match {
    case Some(json) => {
      val result = invocableEngine.invokeMethod(jsonObject.get, "stringify", jsValue.asInstanceOf[AnyRef])

      // TODO: nashorn (java 8) will give result==null for lists and objects, hence this fallback for now:
      if (result == null)
        jsValue.toString
      else
        result.toString
    }
    case None => jsValue.toString
  }

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

  val pileupCountScriptCache = new collection.mutable.HashMap[String, CompiledScript]
  val pileupCountScriptCacheMaxSize = 1000
  class ScriptPileup(pileup: Pileup) extends Pileup(
    pileup.referenceName, pileup.locus, pileup.referenceBase, pileup.elements) {

    override def toString() = "<Pileup at %s:%d of %d elements>".format(referenceName, locus, elements.size)

    def count(code: String): Int = {
      if (code.isEmpty) {
        pileup.elements.size
      } else {
        val compiled = if (pileupCountScriptCache.contains(code)) {
          pileupCountScriptCache(code)
        } else {
          if (pileupCountScriptCache.size > pileupCountScriptCacheMaxSize) {
            pileupCountScriptCache.clear
          }
          val compiled = compilingEngine.compile(code)
          pileupCountScriptCache.put(code, compiled)
          compiled
        }
        val bindings = new SimpleBindings()
        var count = 0
        pileup.elements.foreach(element => {
          bindings.put("element", element)
          bindings.put("read", element.read)
          PartialFunction.condOpt(compiled.eval(bindings): Any) {
            case b: Boolean => if (b) count += 1
            case _          => throw new RuntimeException("Non-boolean value from evaluating: %s".format(code))
          }
        })
        count
      }
    }
  }

  class ScriptRead(read: Read) {
    lazy val token = read.token
    lazy val sequence = Bases.basesToString(read.sequence)
    lazy val baseQualities = read.baseQualities.map(_.toInt)
    lazy val isDuplicate = read.isDuplicate
    lazy val sampleName = read.sampleName
    lazy val isMapped = read.isMapped

    // Mapped read properties
    private lazy val mappedRead: MappedRead = read match {
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

    // Paired read properties
    private lazy val pairedRead: PairedRead[_] = read match {
      case r: PairedRead[_] => r
      case _                => throw new IncompatibleClassChangeError(("Not a paired read: " + toString))
    }
    lazy val isFirstInPair = pairedRead.isFirstInPair
    lazy val isMateMapped = pairedRead.isMateMapped
    lazy val mateStart = pairedRead.mateAlignmentProperties.get.start
    lazy val mateReferenceContig = pairedRead.mateAlignmentProperties.get.referenceContig
    lazy val mateContig = mateReferenceContig
    lazy val mateIsPositiveStrand = pairedRead.mateAlignmentProperties.get.isPositiveStrand
    lazy val inferredInsertSize = pairedRead.mateAlignmentProperties.get.inferredInsertSize

    // Extra properties
    def reverseComplementBases: String = {
      Bases.basesToString(Bases.reverseComplement(read.sequence))
    }
  }

  class ScriptMappedRead(read: MappedRead) extends MappedRead(
    read.token,
    read.sequence: Seq[Byte],
    read.baseQualities: Seq[Byte],
    read.isDuplicate: Boolean,
    read.sampleName: String,
    read.referenceContig: String,
    read.alignmentQuality: Int,
    read.start: Long,
    read.cigar: Cigar,
    read.mdTagString: String,
    read.failedVendorQualityChecks: Boolean,
    read.isPositiveStrand: Boolean,
    read.isPaired) {

    def bases: String = {
      Bases.basesToString(sequence)
    }

  }
}
