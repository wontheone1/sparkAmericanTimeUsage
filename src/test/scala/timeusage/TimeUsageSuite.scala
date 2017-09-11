package timeusage

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ColumnName, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {


  LogManager.getRootLogger.setLevel(Level.ERROR)
  lazy val timeUsage = TimeUsage

  lazy val (columns, initDf) = timeUsage.read("/timeusage/testTimeUsage.csv")
  lazy val (primaryNeedsColumns, workColumns, otherColumns) = timeUsage.classifiedColumns(columns)
  lazy val summaryDf:DataFrame = timeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
  lazy val finalDf: DataFrame = timeUsage.timeUsageGrouped(summaryDf)

  test("timeUsage") {
    assert(timeUsage.spark.sparkContext.appName === "Time Usage")
    assert(timeUsage.spark.sparkContext.isStopped === false)
  }

  test("dfSchema") {
    val testSchema = timeUsage.dfSchema(List("fieldA", "fieldB", "fieldC"))
    assert(testSchema.fields(0).name === "fieldA")
    assert(testSchema.fields(0).dataType === StringType)
    assert(testSchema.fields(1).name === "fieldB")
    assert(testSchema.fields(1).dataType === DoubleType)
    assert(testSchema.fields(2).name === "fieldC")
    assert(testSchema.fields(2).dataType === DoubleType)
  }

  test("row") {
    val testRow = timeUsage.row(List("fieldA", "0.3", "1"))
    assert(testRow(0).getClass.getName === "java.lang.String")
    assert(testRow(1).getClass.getName === "java.lang.Double")
    assert(testRow(2).getClass.getName === "java.lang.Double")
  }

  test("classifiedColumns") {
    val primaryNeeds = primaryNeedsColumns.map(_.toString)
    val works = workColumns.map(_.toString)
    val others = otherColumns.map(_.toString)

    assert(primaryNeeds.contains("t010199"))
    assert(primaryNeeds.contains("t030501"))
    assert(primaryNeeds.contains("t110101"))
    assert(primaryNeeds.contains("t180382"))
    assert(works.contains("t050103"))
    assert(works.contains("t180589"))
    assert(others.contains("t020101"))
    assert(others.contains("t180699"))

  }

  test("timeUsageSummary"){
    assert(summaryDf.columns.length === 6)
    assert(summaryDf.count === 9) // filtering unemployable people results in reduced record number
    summaryDf.show()
  }

  test("timeUsageGrouped") {
    assert(finalDf.count === 5)
    assert(finalDf.head.getDouble(3) === 13.1)
    finalDf.show()
  }

}
