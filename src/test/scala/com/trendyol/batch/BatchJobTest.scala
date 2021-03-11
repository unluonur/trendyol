package com.trendyol.batch

import com.trendyol.models.{Event, EventCount, ProductViewCount, User, UserEvent, UserProductView}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

class BatchJobTest extends AbstractTestBase {
  var env:ExecutionEnvironment = _

  @Before
  def setUp(): Unit = {
    env = ExecutionEnvironment.getExecutionEnvironment
  }

  @Test
  def testCalculateProductViews(): Unit = {
    val events = env.fromElements(
      Event(0L, 1, "view", 1),
      Event(0L, 2, "view", 2),
      Event(0L, 1, "view", 3),
      Event(0L, 2, "click", 2)
    )

    val actual = BatchJob.calculateProductViewCounts(events)
      .collect()
      .toSet
    val expected = Set(
      ProductViewCount(1, 2L),
      ProductViewCount(2, 1L)
    )
    assertEquals(expected, actual)
  }

  @Test
  def testCalculateEventCounts(): Unit = {
    val events = env.fromElements(
      Event(0L, 1, "view", 1),
      Event(0L, 2, "view", 2),
      Event(0L, 1, "view", 3),
      Event(0L, 2, "click", 2)
    )

    val actual = BatchJob.calculateEventCounts(events)
      .collect()
      .toSet
    val expected = Set(
      EventCount("view", 3L),
      EventCount("click", 1L)
    )
    assertEquals(expected, actual)
  }

  @Test
  def testCalculateTop5Users(): Unit = {
    val events = env.fromElements(
      Event(0L, 1, "add", 1),
      Event(0L, 2, "click", 1),
      Event(0L, 3, "remove", 1),
      Event(0L, 4, "view", 1),
      Event(0L, 4, "view", 1),
      Event(0L, 4, "view", 1),
      Event(0L, 4, "view", 1),
      Event(0L, 4, "view", 1),
      Event(0L, 4, "view", 1),

      Event(0L, 1, "add", 2),
      Event(0L, 2, "click", 2),
      Event(0L, 3, "remove", 2),
      Event(0L, 4, "view", 2),
      Event(0L, 4, "view", 2),
      Event(0L, 4, "view", 2),
      Event(0L, 4, "view", 2),
      Event(0L, 4, "view", 2),

      Event(0L, 1, "add", 3),
      Event(0L, 2, "click", 3),
      Event(0L, 3, "remove", 3),
      Event(0L, 4, "view", 3),
      Event(0L, 4, "view", 3),
      Event(0L, 4, "view", 3),
      Event(0L, 4, "view", 3),

      Event(0L, 1, "add", 4),
      Event(0L, 2, "click", 4),
      Event(0L, 3, "remove", 4),
      Event(0L, 4, "view", 4),
      Event(0L, 4, "view", 4),
      Event(0L, 4, "view", 4),

      Event(0L, 1, "add", 5),
      Event(0L, 2, "click", 5),
      Event(0L, 3, "remove", 5),
      Event(0L, 4, "view", 5),
      Event(0L, 4, "view", 5),

      Event(0L, 1, "add", 6),
      Event(0L, 2, "click", 6),
      Event(0L, 3, "remove", 6),
      Event(0L, 4, "view", 6),

      Event(0L, 1, "add", 7),
      Event(0L, 2, "click", 7),
      Event(0L, 3, "remove", 7),
    )
    val actual = BatchJob.calculateTop5Users(events)
      .collect()
      .toList
    val expected = List(
      User(1),
      User(2),
      User(3),
      User(4),
      User(5),
    )
    assertEquals(expected, actual)
  }

  @Test
  def testAllEventsOfUser(): Unit = {
    val events = env.fromElements(
      Event(0L, 1, "view", 1),
      Event(0L, 2, "view", 2),
      Event(0L, 1, "view", 3),
      Event(0L, 3, "click", 2)
    )

    val actual = BatchJob.findUserEvents(events, 2)
      .collect()
      .toSet
    val expected = Set(
      UserEvent("view", 2),
      UserEvent("click", 3)
    )
    assertEquals(expected, actual)
  }

  @Test
  def testUserProductViews(): Unit = {
    val events = env.fromElements(
      Event(0L, 1, "view", 1),
      Event(0L, 2, "view", 2),
      Event(0L, 1, "view", 3),
      Event(0L, 3, "click", 2),
      Event(0L, 4, "view", 2)
    )

    val actual = BatchJob.findUserProductViews(events, 2)
      .collect()
      .toSet
    val expected = Set(
      UserProductView(2),
      UserProductView(4)
    )
    assertEquals(expected, actual)
  }

}
