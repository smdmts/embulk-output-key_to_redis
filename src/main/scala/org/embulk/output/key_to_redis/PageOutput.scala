package org.embulk.output.key_to_redis

import java.security.MessageDigest

import com.google.common.base.Optional
import org.embulk.config.{TaskReport, TaskSource}
import org.embulk.output.key_to_redis.column._
import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi._
import org.bouncycastle.util.encoders.Hex
import org.embulk.output.key_to_redis.redis.Redis

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}

case class PageOutput(taskSource: TaskSource,
                      schema: Schema,
                      putAsMD5: Boolean)
    extends TransactionalPageOutput {
  val task: PluginTask = taskSource.loadTask(classOf[PluginTask])
  val digestMd5: MessageDigest = MessageDigest.getInstance("MD5")

  def timestampFormatter(): TimestampFormatter =
    new TimestampFormatter(task, Optional.absent())

  val buffer = new ListBuffer[Future[Long]]
  val redis: Redis = KeyToRedisOutputPlugin.redis.getOrElse(sys.error("could not find redis."))

  override def add(page: Page): Unit = {
    val reader: PageReader = new PageReader(schema)
    reader.setPage(page)
    while (reader.nextRecord()) {
      val setValueVisitor = SetValueColumnVisitor(
        reader,
        timestampFormatter(),
        task.getKeyWithIndex.asScala.toMap,
        task.getJsonKeyWithIndex.asScala.toMap,
        task.getAppender)
      schema.visitColumns(setValueVisitor)
      val value = setValueVisitor.getValue
      if (value.nonEmpty) {
        if (putAsMD5) {
          val hash = Hex.toHexString(
            digestMd5.digest(setValueVisitor.getValue.getBytes()))
          buffer.append(redis.sadd(hash))
        } else {
          buffer.append(redis.sadd(setValueVisitor.getValue))
        }
      }
    }
    reader.close()
  }

  override def finish(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val sequence = Future.sequence(buffer)
    Await.result(sequence, Duration.Inf)
  }

  override def close(): Unit = ()
  override def commit(): TaskReport = Exec.newTaskReport
  override def abort(): Unit = ()
}
