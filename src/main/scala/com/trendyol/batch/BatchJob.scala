package com.trendyol.batch

import com.trendyol.models.{Event, EventCount, ProductViewCount, User, UserEvent, UserProductView}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._

import java.nio.file.Paths

object BatchJob {

  def readInput(env:ExecutionEnvironment, inputFile:String): DataSet[Event] = {
    env.readCsvFile[Event](inputFile,
      fieldDelimiter = "|",
      ignoreFirstLine = true)
      .distinct()
  }

  // 1. Unique product view counts by ProductId
  def calculateProductViewCounts(events: DataSet[Event]): DataSet[ProductViewCount] = {
    events
      .filter(_.eventName == "view")
      .map(event => ProductViewCount(event.productId, 1))
      .groupBy(0)
      .reduce((left, right) => ProductViewCount(left.productId, left.viewCount + right.viewCount))
  }

  // 2. Unique event counts
  def calculateEventCounts(events: DataSet[Event]): DataSet[EventCount] = {
    events
      .map(event => EventCount(event.eventName, 1))
      .groupBy(0)
      .reduce((left, right) => EventCount(left.eventName, left.eventCount + right.eventCount))
  }

  // 3. Top 5 users who fulfilled all the events (view, add, remove, click)
  def calculateTop5Users(events: DataSet[Event]): DataSet[User] = {
    events
      .map(event => (event.userId, event.eventName, 1))
      .groupBy(0, 1)
      .reduce((left, right) => (left._1, left._2, left._3 + right._3))
      .map(event => (event._1, 1, event._3))
      .groupBy(0)
      .reduce((left, right) => (left._1, left._2 + right._2, left._3 + right._3))
      .filter(_._2 == 4)
      .setParallelism(1)
      .sortPartition(2, Order.DESCENDING)
      .first(5)
      .map(row => User(row._1))
  }

  // 4. All events of #UserId: 47
  def findUserEvents(events: DataSet[Event], userId: Int): DataSet[UserEvent] = {
    events
      .filter(_.userId == userId)
      .map(event => UserEvent(event.eventName, event.productId))
      .distinct()
  }

  // 5. Product views of #UserId: 47
  def findUserProductViews(events: DataSet[Event], userId: Int): DataSet[UserProductView] = {
    events
      .filter(event => event.userId == userId && event.eventName == "view")
      .map(event => UserProductView(event.productId))
      .distinct()
  }

  def writeCsvFile(dataSet: DataSet[_], outputFile:String): Unit = {
    dataSet.writeAsCsv(outputFile,
      fieldDelimiter = "|",
      writeMode = WriteMode.OVERWRITE)
      .setParallelism(1)
  }

  def run(inputFile:String, outputPath:String): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val events = readInput(env, inputFile)

    writeCsvFile(calculateProductViewCounts(events), Paths.get(outputPath, "1_product_view_counts.csv").toString)

    writeCsvFile(calculateEventCounts(events), Paths.get(outputPath, "2_event_counts.csv").toString)

    writeCsvFile(calculateTop5Users(events), Paths.get(outputPath, "3_users.csv").toString)

    writeCsvFile(findUserEvents(events, 47), Paths.get(outputPath, "4_user_47_events.csv").toString)

    writeCsvFile(findUserProductViews(events, 47), Paths.get(outputPath, "5_user_47_product_views.csv").toString)

    env.execute()
  }

  case class Arguments(inputFile:String = "", outputPath:String = "")

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Arguments]("My Application") {
      override def errorOnUnknownArgument = false

      opt[String]('i', "input-file")
        .required()
        .valueName("<input-file>")
        .action((input, arguments) => arguments.copy(inputFile = input))
        .text("input-file is required")

      opt[String]('o', "output-path")
        .required()
        .valueName("<output-path>")
        .action((output, arguments) => arguments.copy(outputPath = output))
        .text("output-path is required")
    }
    parser.parse(args, Arguments()) match {
      case Some(arguments) =>
        run(arguments.inputFile, arguments.outputPath)
      case _ =>
    }
  }
}
