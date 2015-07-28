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
import javax.script.{ CompiledScript, Compilable, SimpleBindings, Bindings }

import com.google.common.io.CharStreams
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.Common.Arguments.{ NoSequenceDictionary }
import org.hammerlab.guacamole.commands.Evaluation.{ ScriptPileup }
import org.hammerlab.guacamole.pileup.{ PileupElement, Pileup }
import org.hammerlab.guacamole.reads.{ MappedRead, Read }
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
      usage = "Input to evaluate, either: 'reads' or 'pileups' (default)")
    var domain: String = "pileups"

    @Args4jOption(name = "--include-file", usage = "Code files to include", handler = classOf[StringArrayOptionHandler])
    var includeFiles: ArrayBuffer[String] = ArrayBuffer.empty

    var includeCode = new ArrayBuffer[String]()
    @Args4jOption(name = "--include-code",
      usage = "Code to include")
    def addCode(arg: String): Unit = includeCode += arg

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
                expressionLabels += field
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

    val standardInclude = CharStreams.toString(
      new InputStreamReader(ClassLoader.getSystemClassLoader.getResourceAsStream("eval_command_preamble.js")))

    def makeScript(args: Arguments) = {
      lazy val filesystem = FileSystem.get(new Configuration())
      val extraIncludesFromFiles = args.includeFiles.map(path => {
        val code = IOUtils.toString(new InputStreamReader(filesystem.open(new Path(path))))
        "// Included from: %s\n%s".format(path, code)
      })
      val extraIncludesFromCommandline = args.includeCode.zipWithIndex.map(pair => {
        "// Included code block %d\n%s".format(pair._2 + 1, pair._1)
      })
      (Iterator(standardInclude) ++ extraIncludesFromFiles ++ extraIncludesFromCommandline).mkString("\n")
    }

    override def run(args: Arguments, sc: SparkContext): Any = {
      val (bamLabels, bams, expressionLabels, expressions) = parseFields(args.fields)
      assert(bamLabels.size == bams.size)
      assert(expressionLabels.size == expressions.size)

      val script = makeScript(args)

      println("Read inputs (%d):".format(bams.size))
      bams.zip(bamLabels).foreach(pair => println("%20s : %s".format(pair._2, pair._1)))
      println()
      println("Script:")
      println(Evaluation.stringWithLineNumbers(script))
      println()
      println("Expressions (%d):".format(expressions.size))
      expressions.zip(expressionLabels).foreach({
        case (label, expression) =>
          println("%20s : %s".format(label, expression))
      })
      println()

      // Test that our script runs and that and expressions at least compile, so we error out immediately if not.
      Evaluation.eval(script, Evaluation.compilingEngine.compile(script), new SimpleBindings())
      expressions.foreach(Evaluation.compilingEngine.compile _)

      val readSets = bams.map(
        path => ReadSet(
          sc, path, false, Read.InputFilters.empty, token = 0, contigLengthsFromDictionary = !args.noSequenceDictionary))

      assert(readSets.forall(_.sequenceDictionary == readSets(0).sequenceDictionary),
        "Samples have different sequence dictionaries: %s."
          .format(readSets.map(_.sequenceDictionary.toString).mkString("\n")))

      val loci = Common.loci(args, readSets(0))
      val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(args, loci, readSets.map(_.mappedReads): _*)

      val separator = args.separator

      val result = DistributedUtil.pileupFlatMapMultipleRDDsWithState[String, Option[(Seq[CompiledScript], Bindings)]](
        readSets.map(_.mappedReads),
        lociPartitions,
        !args.includeEmpty, // skip empty
        initialState = None,
        (maybeState, pileups) => {
          val (compiledExpressions, bindings) = maybeState match {
            case None => {
              val newBindings = new SimpleBindings()
              Evaluation.eval(script, Evaluation.compilingEngine.compile(script), newBindings)
              (expressions.map(Evaluation.compilingEngine.compile _), newBindings)
            }
            case Some(pair) => pair
          }

          // Update bindings for the current locus.
          val scriptPileups = pileups.map(new ScriptPileup(_))
          bindings.put("ref", Bases.baseToString(pileups(0).referenceBase))
          bindings.put("locus", pileups(0).locus)
          bindings.put("contig", pileups(0).referenceName)
          bindings.put("pileups", scriptPileups)
          bindings.put("_by_label", bamLabels.zip(scriptPileups).toMap)

          val evaluatedExpressionResults = expressions.zip(compiledExpressions).map(
            pair => Evaluation.eval(pair._1, pair._2, bindings))

          val result = if (evaluatedExpressionResults.exists(_ == null)) {
            Iterator.empty
          } else {
            Iterator.apply(evaluatedExpressionResults.map(_.toString).mkString(separator))
          }
          (Some((compiledExpressions, bindings)), result)
        })

      if (args.out.nonEmpty) {
        sc.parallelize(Seq(expressionLabels.mkString(separator))).union(result).saveAsTextFile(args.out)
        ""
      } else {

        println("BEGIN RESULT")
        println("*" * 60)
        println(expressionLabels.mkString(separator))
        result.collect.foreach(println _)
        println("*" * 60)
        println("END RESULT")
      }

      DelayedMessages.default.print()

      // We return the result to make unit testing easier.
      result
    }
  }
}
object Evaluation {
  val factory = new javax.script.ScriptEngineManager
  val engine = factory.getEngineByName("JavaScript")
  val compilingEngine = engine.asInstanceOf[Compilable]

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
}
