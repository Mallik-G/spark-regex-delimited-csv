/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.anish.spark.regexcsv

import java.io.IOException
import java.text.SimpleDateFormat

import org.anish.spark.regexcsv.readers.RegexDelimitedLineCsvReader
import org.anish.spark.regexcsv.util._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

/**
  * Originally from Databricks Spark CSV data source. Modified to support Regex delimiters.
  *
  * Modified by anish on 10/01/2018.
 */
case class CsvRelation protected[regexcsv]
(
  baseRDD: () => RDD[String],
  location: Option[String],
  useHeader: Boolean,
  delimiter: String,
  quote: Character,
  escape: Character,
  comment: Character,
  parseMode: String,
  treatEmptyValuesAsNulls: Boolean,
  userSchema: StructType = null,
  inferCsvSchema: Boolean,
  codec: String = null,
  nullValue: String = "",
  dateFormat: String = null,
  maxCharsPerCol: Int = 100000)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan with InsertableRelation {

  // Share date format object as it is expensive to parse date pattern.
  private val dateFormatter = if (dateFormat != null) new SimpleDateFormat(dateFormat) else null

  private val logger = LoggerFactory.getLogger(CsvRelation.getClass)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  private val regexDelimitedLineCsvReader: RegexDelimitedLineCsvReader = {
    val escapeVal = if (escape == null) '\\' else escape.charValue()
    val commentChar: Char = if (comment == null) '\0' else comment
    val quoteChar: Char = if (quote == null) '\0' else quote
    new RegexDelimitedLineCsvReader(
      fieldSep = delimiter,
      quote = quoteChar,
      escape = escapeVal,
      commentMarker = commentChar)
  }

  override val schema: StructType = inferSchema()

  /**
    * Tokenize the input RDD[String]
    */
  private def tokenRdd(header: Array[String]): RDD[Array[String]] = {
    val filterLine = if (useHeader) firstLine else null

    baseRDD().mapPartitions { iter =>
      // When using header, any input line that equals firstLine is assumed to be header
      val csvIter = if (useHeader) {
        iter.filter(_ != filterLine) // Filter out the header lines
      } else {
        iter
      }
      csvIter.map { line =>
        regexDelimitedLineCsvReader.parseLine(line)
      }
    }
  }

  override def buildScan: RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val rowArray = new Array[Any](schemaFields.length)
    tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

      if (dropMalformed && schemaFields.length != tokens.length) {
        logger.warn(s"Dropping malformed line: ${tokens.mkString(",")}")
        None
      } else if (failFast && schemaFields.length != tokens.length) {
        throw new RuntimeException(s"Malformed line in FAILFAST mode: ${tokens.mkString(",")}")
      } else {
        var index: Int = 0
        try {
          index = 0
          while (index < schemaFields.length) {
            val field = schemaFields(index)
            rowArray(index) = TypeCast.castTo(tokens(index), field.dataType, field.nullable,
              treatEmptyValuesAsNulls, nullValue, simpleDateFormatter)
            index = index + 1
          }
          Some(Row.fromSeq(rowArray))
        } catch {
          case aiob: ArrayIndexOutOfBoundsException if permissive =>
            (index until schemaFields.length).foreach(ind => rowArray(ind) = null)
            Some(Row.fromSeq(rowArray))
          case _: java.lang.NumberFormatException |
               _: IllegalArgumentException if dropMalformed =>
            logger.warn("Number format exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
          case pe: java.text.ParseException if dropMalformed =>
            logger.warn("Parse exception. " +
              s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
            None
        }
      }
    }
  }


  /**
    * This supports to eliminate unneeded columns before producing an RDD
    * containing all of its tuples as Row objects. This reads all the tokens of each line
    * and then drop unneeded tokens without casting and type-checking by mapping
    * both the indices produced by `requiredColumns` and the ones of tokens.
    */
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields
    val shouldTableScan = schemaFields.deep == requiredFields.deep
    val safeRequiredFields = if (dropMalformed) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    } else {
      requiredFields
    }
    val rowArray = new Array[Any](safeRequiredFields.length)
    if (shouldTableScan) {
      buildScan()
    } else {
      val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
      schemaFields.zipWithIndex.filter {
        case (field, _) => safeRequiredFields.contains(field)
      }.foreach {
        case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
      }
      val requiredSize = requiredFields.length
      tokenRdd(schemaFields.map(_.name)).flatMap { tokens =>

        if (dropMalformed && schemaFields.length != tokens.length) {
          logger.warn(s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
          None
        } else if (failFast && schemaFields.length != tokens.length) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
            s"${tokens.mkString(delimiter.toString)}")
        } else {
          val indexSafeTokens = if (permissive && schemaFields.length > tokens.length) {
            tokens ++ new Array[String](schemaFields.length - tokens.length)
          } else if (permissive && schemaFields.length < tokens.length) {
            tokens.take(schemaFields.length)
          } else {
            tokens
          }
          try {
            var index: Int = 0
            var subIndex: Int = 0
            while (subIndex < safeRequiredIndices.length) {
              index = safeRequiredIndices(subIndex)
              val field = schemaFields(index)
              rowArray(subIndex) = TypeCast.castTo(
                indexSafeTokens(index),
                field.dataType,
                field.nullable,
                treatEmptyValuesAsNulls,
                nullValue,
                simpleDateFormatter
              )
              subIndex = subIndex + 1
            }
            Some(Row.fromSeq(rowArray.take(requiredSize)))
          } catch {
            case _: java.lang.NumberFormatException |
                 _: IllegalArgumentException if dropMalformed =>
              logger.warn("Number format exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            case pe: java.text.ParseException if dropMalformed =>
              logger.warn("Parse exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
          }
        }
      }
    }
  }

  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val firstRow = regexDelimitedLineCsvReader.parseLine(firstLine)
      val header = if (useHeader) {
        firstRow
      } else {
        firstRow.zipWithIndex.map { case (value, index) => s"C$index" }
      }
      if (this.inferCsvSchema) {
        val simpleDateFormatter = dateFormatter
        InferSchema(
          tokenRdd(header),
          header,
          ParseModes.isDropMalformedMode(parseMode),
          nullValue,
          simpleDateFormatter)
      } else {
        // By default fields are assumed to be StringType
        val schemaFields = header.map { fieldName =>
          StructField(fieldName.toString, StringType, nullable = true)
        }
        StructType(schemaFields)
      }
    }
  }

  /**
    * Returns the first line of the first non-empty file in path
    */
  private lazy val firstLine = {
    if (comment != null) {
      baseRDD().filter { line =>
        line.trim.nonEmpty && !line.startsWith(comment.toString)
      }.first()
    } else {
      baseRDD().filter { line =>
        line.trim.nonEmpty
      }.first()
    }
  }


  // The function below was borrowed from JSONRelation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {

    val filesystemPath = location match {
      case Some(p) => new Path(p)
      case None =>
        throw new IOException(s"Cannot INSERT into table with no path defined")
    }

    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      try {
        fs.delete(filesystemPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to INSERT OVERWRITE a CSV table:\n${e.toString}")
      }
      // Write the data. We assume that schema isn't changed, and we won't update it.

      val codecClass = CompressionCodecs.getCodecClass(codec)
      data.saveAsCsvFile(filesystemPath.toString, Map("delimiter" -> delimiter.toString),
        codecClass)
    } else {
      sys.error("CSV tables only support INSERT OVERWRITE for now.")
    }
  }
}
