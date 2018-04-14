package org.reactorlabs.jshealth.models

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Schemas{
  val asMap: Map[String, (StructType, Seq[String])] = Map(
    "fht"       -> (StructType(Array(
      StructField("REPO_OWNER",   StringType, nullable = false),
      StructField("REPOSITORY",   StringType, nullable = false),
      StructField("GIT_PATH",     StringType, nullable = false),
      StructField("HASH_CODE",    StringType, nullable = true),
      StructField("COMMIT_TIME",  LongType,   nullable = false),
      StructField("COMMIT_ID",    StringType, nullable = false)))
      , Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH", "HASH_CODE", "COMMIT_ID")),
    "commitMsg" -> (StructType(Array(
      StructField("COMMIT_ID",    StringType, nullable = false),
      StructField("AUTHOR",       StringType, nullable = false),
      StructField("LONG_MESSAGE", StringType, nullable = false)))
      , Seq("COMMIT_ID")),
    "indexes"   -> (StructType(Array(StructField("HASH_CODE", StringType, nullable = false)))
      , Seq("HASH_CODE")),
    "contents"  -> (StructType(Array(
      StructField("HASH_CODE", StringType, nullable = false),
      StructField("CONTENTS",  StringType, nullable = false)))
      , Seq("HASH_CODE"))
  )
}
