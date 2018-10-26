package com.logisima.spark

import com.logisima.caps.Neo4jImporterGraphDataSource
import com.logisima.spark.ConsoleApp.spark
import org.apache.spark.sql.functions.{monotonically_increasing_id, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}

object ConsoleApp extends App {

  // Redirects output to the current Console. This is needed for testing.
  implicit val printOptions: PrintOptions = PrintOptions(Console.out)

  // 1) Create CAPS session and retrieve Spark session
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  implicit val session: CAPSSession = CAPSSession.create(spark)


  val graph = session.readFrom(
    ImdbToGraph.nodesMovie(spark),
    ImdbToGraph.nodesGenre(spark),
    ImdbToGraph.nodesPerson(spark),
    ImdbToGraph.relsMovieInGenre(spark),
    ImdbToGraph.relsPersonActedIn(spark),
    ImdbToGraph.relsPersonDirected(spark),
    ImdbToGraph.relsPersonProduced(spark),
    ImdbToGraph.relsPersonCinematographed(spark),
    ImdbToGraph.relsPersonEdited(spark),
    ImdbToGraph.relsPersonComposed(spark),
    ImdbToGraph.relsPersonWritten(spark)
  )

  new Neo4jImporterGraphDataSource()(session)
    .store(graph, "./data")

//    val neo4jConfig: Neo4jConfig = new Neo4jConfig(new URI("bolt://localhost:7687"), "neo4j", Some("admin"), true)
//    val neo4jSource: Neo4jPropertyGraphDataSource = new Neo4jPropertyGraphDataSource(neo4jConfig)(session)
//    neo4jSource.store()

}

case object ImdbToGraph {

  import spark.implicits._

  val folder = "./csv_full/"
  val nullString = "\\N"

  // ~~~`
  // ~~~ Create Nodes
  // ~~~

  def nodesMovie(spark: SparkSession): CAPSNodeTable = {
    CAPSNodeTable.fromMapping(
      NodeMapping
        .withSourceIdKey("id")
        .withImpliedLabel("Movie")
        .withPropertyKeys("isAdult")
        .withPropertyKeys("released")
        .withPropertyKeys("endDate")
        .withPropertyKeys("title")
        .withPropertyKeys("duration")
        .withPropertyKeys("averageRating")
        .withPropertyKeys("numVotes")
      ,
      read(spark, "title.basics.tsv")
        .join(
          read(spark, "title.ratings.tsv"),
          Seq("tconst"),
          "left_outer"
        )
        .map(row => (
          colToMovieId(row, "tconst"),
          colToInt(row, "startYear"),
          colToInt(row, "endYear"),
          colToBoolean(row, "isAdult"),
          colToString(row, "primaryTitle"),
          colToInt(row, "runtimeMinutes"),
          colToFloat(row, "averageRating"),
          colToInt(row, "numVotes")
        ))
        .toDF("id", "released", "endDate", "isAdult", "title", "duration", "averageRating", "numVotes")
    )
  }

  def nodesGenre(spark: SparkSession): CAPSNodeTable = {
    CAPSNodeTable.fromMapping(
      NodeMapping
        .withSourceIdKey("id")
        .withImpliedLabel("Genre")
        .withPropertyKeys("name")
      ,
      read(spark, "title.basics.tsv")
        .flatMap(row => colToStringArray(row, "genres") match {
          case Some(value) => value
          case None => None
        })
        .distinct()
        .map(row => (toGenreId(row), row))
        .toDF("id", "name")
    )
  }

  def nodesPerson(spark: SparkSession): CAPSNodeTable = {
    CAPSNodeTable.fromMapping(
      NodeMapping
        .withSourceIdKey("id")
        .withImpliedLabel("Person")
        .withPropertyKeys("birth")
        .withPropertyKeys("death")
        .withPropertyKeys("name")
      ,
      read(spark, "name.basics.tsv")
        .map(row => (
          colToPersonId(row, "nconst"),
          colToInt(row, "birthYear"),
          colToInt(row, "deathYear"),
          colToString(row, "primaryName")
        ))
        .toDF("id", "birth", "death", "name")
    )
  }

  // ~~~
  // ~~~ Create Relationships
  // ~~~

  def relsMovieInGenre(spark: SparkSession): CAPSRelationshipTable = {
    CAPSRelationshipTable.fromMapping(
      RelationshipMapping
        .withSourceIdKey("id")
        .withSourceStartNodeKey("movieId")
        .withSourceEndNodeKey("genreId")
        .withRelType("IN_GENRE")
      ,
      read(spark, "title.basics.tsv")
        .flatMap(row =>
          colToStringArray(row, "genres") match {
            case Some(value) => value.map(genre => (
              colToMovieId(row, "tconst"),
              toGenreId(genre)
            ))
            case None => None
          }
        )
        .toDF("movieId", "genreId")
        .withColumn("id", monotonically_increasing_id())
    )
  }

  def relsPersonActedIn(spark: SparkSession): CAPSRelationshipTable = {
    CAPSRelationshipTable.fromMapping(
      RelationshipMapping
        .withSourceIdKey("id")
        .withSourceStartNodeKey("personId")
        .withSourceEndNodeKey("movieId")
        .withRelType("ACTED_IN")
        .withPropertyKey("roles")
      ,
      read(spark, "title.principals.tsv")
        .filter(col("category") === "actor" || col("category") === "actress" || col("category") === "self")
        .map(row => (
          colToPersonId(row, "nconst"),
          colToMovieId(row, "tconst"),
          colToStringArray(row, "characters")
        ))
        .toDF("personId", "movieId", "roles")
        .withColumn("id", monotonically_increasing_id())
    )
  }

  def relsPersonProduced(spark: SparkSession): CAPSRelationshipTable = {
    CAPSRelationshipTable.fromMapping(
      RelationshipMapping
        .withSourceIdKey("id")
        .withSourceStartNodeKey("personId")
        .withSourceEndNodeKey("movieId")
        .withRelType("PRODUCED")
      ,
      read(spark, "title.principals.tsv")
        .filter(col("category") === "producer")
        .map(row => (
          colToPersonId(row, "nconst"),
          colToMovieId(row, "tconst")
        ))
        .toDF("personId", "movieId")
        .withColumn("id", monotonically_increasing_id())
    )
  }

  def relsPersonDirected(spark: SparkSession): CAPSRelationshipTable = {
    CAPSRelationshipTable.fromMapping(
      RelationshipMapping
        .withSourceIdKey("id")
        .withSourceStartNodeKey("personId")
        .withSourceEndNodeKey("movieId")
        .withRelType("DIRECTED")
      ,
      read(spark, "title.principals.tsv")
        .filter(col("category") === "director")
        .map(row => (
          colToPersonId(row, "nconst"),
          colToMovieId(row, "tconst")
        ))
        .toDF("personId", "movieId")
        .withColumn("id", monotonically_increasing_id())
    )
  }

  def relsPersonComposed(spark: SparkSession): CAPSRelationshipTable = {
    CAPSRelationshipTable.fromMapping(
      RelationshipMapping
        .withSourceIdKey("id")
        .withSourceStartNodeKey("personId")
        .withSourceEndNodeKey("movieId")
        .withRelType("COMPOSED")
      ,
      read(spark, "title.principals.tsv")
        .filter(col("category") === "composer")
        .map(row => (
          colToPersonId(row, "nconst"),
          colToMovieId(row, "tconst")
        ))
        .toDF("personId", "movieId")
        .withColumn("id", monotonically_increasing_id())
    )
  }

  def relsPersonEdited(spark: SparkSession): CAPSRelationshipTable = {
    CAPSRelationshipTable.fromMapping(
      RelationshipMapping
        .withSourceIdKey("id")
        .withSourceStartNodeKey("personId")
        .withSourceEndNodeKey("movieId")
        .withRelType("EDITED")
      ,
      read(spark, "title.principals.tsv")
        .filter(col("category") === "editor")
        .map(row => (
          colToPersonId(row, "nconst"),
          colToMovieId(row, "tconst")
        ))
        .toDF("personId", "movieId")
        .withColumn("id", monotonically_increasing_id())
    )
  }

  def relsPersonCinematographed(spark: SparkSession): CAPSRelationshipTable = {
    CAPSRelationshipTable.fromMapping(
      RelationshipMapping
        .withSourceIdKey("id")
        .withSourceStartNodeKey("personId")
        .withSourceEndNodeKey("movieId")
        .withRelType("CINEMATOGRAPHED")
      ,
      read(spark, "title.principals.tsv")
        .filter(col("category") === "cinematographer")
        .map(row => (
          colToPersonId(row, "nconst"),
          colToMovieId(row, "tconst")
        ))
        .toDF("personId", "movieId")
        .withColumn("id", monotonically_increasing_id())
    )
  }

  def relsPersonWritten(spark: SparkSession): CAPSRelationshipTable = {
    CAPSRelationshipTable.fromMapping(
      RelationshipMapping
        .withSourceIdKey("id")
        .withSourceStartNodeKey("personId")
        .withSourceEndNodeKey("movieId")
        .withRelType("WRITTEN")
      ,
      read(spark, "title.principals.tsv")
        .filter(col("category") === "writer")
        .map(row => (
          colToPersonId(row, "nconst"),
          colToMovieId(row, "tconst")
        ))
        .toDF("personId", "movieId")
        .withColumn("id", monotonically_increasing_id())
    )
  }


  // ~~~
  // ~~~ Columns to node's ID (node's ID MUST BE unique across the entire graph)
  // ~~~

  def colToMovieId(row: Row, column: String): Long = {
    try {
      row.getAs(column).toString.replaceFirst("tt", "").toLong + 100000000
    }
    catch {
      case e: Exception => {
        println("exception caught: " + e)
        println(row)
        throw e
      }
    }
  }

  def colToPersonId(row: Row, column: String): Long = {
    try {
      row.getAs(column).toString.replaceFirst("nm", "").toLong + 200000000
    }
    catch {
      case e: Exception => {
        println("exception caught: " + e)
        println(row)
        throw e
      }
    }
  }

  def colToTypeId(row: Row, column: String): Long = {
    try {
      row.getAs(column).toString.hashCode.toLong
    }
    catch {
      case e: Exception => {
        println("exception caught: " + e)
        println(row)
        throw e
      }
    }
  }

  def toGenreId(value:String): Long = {
    value.toString.hashCode.toLong
  }


  // ~~~
  // ~~~ Helper functions for managing dataframes
  // ~~~

  def read(spark: SparkSession, file: String): DataFrame = {
    spark.read
      .option("sep", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(folder + file)
  }

  def colToInt(row: Row, column: String): Option[Int] = {
    try {
      if (row.getAs(column) == null || row.getAs(column).toString.equals(nullString))
        None
      else
        Some(row.getAs(column).toString.toInt)
    }
    catch {
      case e: Exception => {
        println("exception caught: " + e)
        println(row)
        throw e
      }
    }
  }

  def colToFloat(row: Row, column: String): Option[Float] = {
    try {
      if (row.getAs(column) == null || row.getAs(column).toString.equals(nullString))
        None
      else
        Some(row.getAs(column).toString.toFloat)
    }
    catch {
      case e: Exception => {
        println("exception caught: " + e)
        println(row)
        throw e
      }
    }
  }

  def colToBoolean(row: Row, column: String): Option[Boolean] = {
    try {
      if (row.getAs(column) == null || row.getAs(column).toString.equals(nullString))
        None
      else
        Some(row.getAs(column).toString.equals("1"))
    }
    catch {
      case e: Exception => {
        println("exception caught: " + e)
        println(row)
        throw e
      }
    }
  }

  def colToString(row: Row, column: String): Option[String] = {
    try {
      if (row.getAs(column) == null || row.getAs(column).toString.equals(nullString))
        None
      else
        Some(row.getAs(column).toString)
    }
    catch {
      case e: Exception => {
        println("exception caught: " + e)
        println(row)
        throw e
      }
    }
  }

  def colToStringArray(row: Row, column: String): Option[Seq[String]] = {
    try {
      if (row.getAs(column) == null || row.getAs(column).equals(nullString))
        None
      else
        Some(
          row.getAs(column)
            .toString
            .split(',')
            .filterNot(value => value.equals(nullString))
            .map(_.trim)
            .toSeq
        )
    }
    catch {
      case e: Exception => {
        println("exception caught: " + e)
        println(row)
        throw e
      }
    }
  }

}
