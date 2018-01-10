# Spark DataSource for reading Regex delimited CSV files

Forked from Databricks [Spark CSV] (https://github.com/databricks/spark-csv)

This data source adds the capability to use any regex as a delimiter when reading a CSV file (or rather a delimited text file)

Tested in Scala 2.11.6 and Spark 2.1.0

### Compile using Maven
```
mvn clean package
```

### Use in code as follows.
```
val df = sparkSession
  .read
  .format("org.anish.spark.regexcsv")
  .option("header", "true")
  .option("delimiter", ",;")
  .load("/path/to/file")
```

### Execute examples as follows

```
java -cp target/spark-regex-delimited-csv-1.0-SNAPSHOT-jar-with-dependencies.jar org.anish.spark.examples.RegexDelimitedCsvExample
```

## Licenses
Not sure about Licenses
