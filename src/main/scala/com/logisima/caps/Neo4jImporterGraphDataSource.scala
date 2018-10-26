package com.logisima.caps

import java.io.{File, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.neo4j.commandline.admin.AdminTool
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSConverters._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * This class allow you to manage graph from the neo4j import tool format.
  * You can :
  *   - create a graph.db from a property graph by using the neo4j import tool
  *   - create the csv files for the neo4j import tool
  *   - create a PropertyGraph from the csv files for the neo4j importer tool
  */
case class Neo4jImporterGraphDataSource()(implicit val caps: CAPSSession) {

  /**
    * Generate the CSV files for the import tool from a PropertyGraph.
    *
    * @param graph The property graph to use as a source
    * @param path  Where the CSV files will be generated
    */
  def writeCsv(graph: PropertyGraph, path: String): Array[String] = {
    // Reset folder
    Writers.deleteFolderContent(path)

    // Importer common arguments
    var args = ListBuffer(
      "import",
      "--ignore-duplicate-nodes", "true",
      "--ignore-missing-nodes", "true",
      "--array-delimiter", "|",
      "--max-memory", "50%"
    )

    // Adding nodes files to the agrs
    Writers.writeNodes(graph, path).foreach(folder => {
      args += "--nodes"
      args += folder + "/header.csv," + folder + "/part.*"
    })

    // Adding rels files to the agrs
    Writers.writeRelationships(graph, path).foreach(folder => {
      args += "--relationships"
      args += folder + "/header.csv," + folder + "/part.*"
    })

    println("neo4j-admin \n\\\t" + args.mkString("\n\\\t"))

    args.toArray

  }

  /**
    * Generate a graph.db folder from a PropertyGraph, by using the neo4j import tool.
    *
    * @param graph The property graph to use as a source
    * @param path  Where the graph.db will be generated
    */
  def store(graph: PropertyGraph, path: String): Unit = {
    // Run the neo4j importer
    AdminTool.main(
      writeCsv(graph, path)
    )
  }

}


case object Writers {

  val join = udf((t: mutable.WrappedArray[Any]) => {
    if (t != null)
      t
        .reduce((a, b) => a + "|" + b)
        .toString
        .replace("[\"", "")
        .replace("\"]", "")
        .replace("\"|\"", "|")
    else ""
  })

  def deleteFolderContent(path: String): Unit = {
    val file: File = new File(path)
    if (file.isDirectory) {
      file.listFiles().foreach(file => {
        deleteFolderContent(file.getPath)
      })
    }
    file.delete()
  }

  def writeNodes(graph: PropertyGraph, path: String)(implicit caps: CAPSSession): Set[String] = {
    graph.schema.labelCombinations.combos.map(combo => {
      val folder = path + "/csv/" + combo.mkString("_")

      // Retrieve the DF node from the PropertyGraph
      val nodeDf = graph
        .nodes("n", CTNode(combo), exactLabelMatch = true)
        .asCaps
        .df

      // Adding the generic `:LABEL` column
      val df = nodeDf
        .toDF(computeHeaders(nodeDf): _*)
        .withColumn(":LABEL", lit(combo.mkString("|")))

      // Removing all the `label:BOOLEAN` column
      val df2 = combo
        .seq
        .foldLeft(df) { (df, label) =>
          df.drop(label + ":BOOLEAN")
        }

      // Transform all array column to string with split character as '|'
      val df3 = df2
        .schema
        .filter(p => p.dataType.typeName == "array")
        .map(p => p.name)
        .foldLeft(df2) { (df, column) =>
          df.withColumn(column, join(df(column)))
        }

      this.writeCsv(df3, folder)

      folder
    })
  }

  def writeRelationships(graph: PropertyGraph, path: String)(implicit caps: CAPSSession): Set[String] = {
    graph.schema.relationshipTypes.map(relType => {
      val folder = path + "/csv/" + relType

      // Retrieve the DF relationship from the PropertyGraph
      val relDf = graph
        .relationships("r", CTRelationship(relType))
        .asCaps
        .df

      // Adding the `:TYPE` column and removing the `:ID` column
      val df = relDf
        .toDF(computeHeaders(relDf): _*)
        .withColumn(":TYPE", lit(relType))
        .drop(":ID")

      // Remove all the  `type()*` column
      val df2 = relDf
        .columns
        .filter(value => value.startsWith("type()"))
        .foldLeft(df) { (df, column) =>
          df.drop(column)
        }

      // Transform all array column to string with split character as '|'
      val df3 = df2
        .schema
        .filter(p => p.dataType.typeName == "array")
        .map(p => p.name)
        .foldLeft(df2) { (df, column) =>
          df.withColumn(column, join(df(column)))
        }

      this.writeCsv(df3, folder)

      folder
    })
  }

  private def computeHeaders(df: DataFrame): Seq[String] = {
    val idNodePattern = " __.*NODE.*".r
    val idRelPattern = " __.*RELATIONSHIP.*".r
    val idSourcePattern = "source\\( __ RELATIONSHIP.*".r
    val idTargetPattern = "target\\( __ RELATIONSHIP.*".r
    val fieldArrayPattern = "^_(.*) __ LIST[\\?]? OF ([^\\?]*)[\\?]?$".r
    val fieldPattern = "^_(.*) __([^\\?]*)[\\?]?$".r

    df
      .columns
      .toSeq
      .map(value => {
        value match {
          case idNodePattern() => ":ID"
          case idRelPattern() => ":ID"
          case idSourcePattern() => ":START_ID"
          case idTargetPattern() => ":END_ID"
          case fieldPattern(fieldName, fieldType) =>
            fieldType.trim match {
              case "INTEGER" => fieldName.trim + ":int"
              case _ => fieldName.trim + ":" + fieldType.trim.toLowerCase
            }
          case fieldArrayPattern(fieldName, fieldType) => {
            fieldType.trim match {
              case "INTEGER" => fieldName.trim + ":int[]"
              case _ => fieldName.trim + ":" + fieldType.trim.toLowerCase + "[]"
            }
          }
          case _ => value
        }
      })
  }

  private def writeCsv(df: DataFrame, path: String): Unit = {
    // write the data CSV
    df
      .write
      .format("csv")
      .option("escape", "\"")
      .save(path)

    // write the header CSV
    new PrintWriter(path + "/header.csv") {
      write(df.columns.mkString(","))
      close
    }
  }

}
