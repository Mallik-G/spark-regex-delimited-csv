package org.anish.spark.regexcsv.readers

import scala.collection.mutable

/**
  * Created by anish on 10/01/2018.
  */
class RegexDelimitedLineCsvReader
(
  fieldSep: String = ",",
  quote: Char = '"',
  escape: Char = '\\',
  commentMarker: Char = '#',
  ignoreLeadingSpace: Boolean = true,
  ignoreTrailingSpace: Boolean = true)
  extends Serializable {
  /**
    * parse a line
    *
    * @param line a String with no newline at the end
    * @return array of strings where each string is a field in the CSV record
    */
  def parseLine(line: String): Array[String] = {
    // reg ex for space and " is [^\s"]+|"([^"]*)"
    val colRegex = s"[^$fieldSep$quote]+|$quote([^$quote]*)$quote"
    // TODO If needed add support for escape characters and comment characters
    val matchList = mutable.ListBuffer[String]()

    import java.util.regex.Pattern
    val regex = Pattern.compile(colRegex)
    val regexMatcher = regex.matcher(line)
    while (regexMatcher.find) {
      if (regexMatcher.group(1) != null) { // Add quoted string without the quotes
        matchList += regexMatcher.group(1)
      }
      else { // Add unquoted word
        matchList += regexMatcher.group
      }
    }
    matchList.toArray
  }

}