package org.embulk.output.key_to_redis

import java.security.MessageDigest

import com.google.common.base.Optional
import org.embulk.config.{TaskReport, TaskSource}
import org.embulk.output.key_to_redis.column._
import org.embulk.spi.time.TimestampFormatter
import org.embulk.spi._
import org.bouncycastle.util.encoders.Hex

import scala.collection.JavaConverters._

case class PageOutput(taskSource: TaskSource,
                      schema: Schema,
                      putAsMD5: Boolean)
    extends TransactionalPageOutput {
  val task: PluginTask = taskSource.loadTask(classOf[PluginTask])
  val digestMd5: MessageDigest = MessageDigest.getInstance("MD5")

  def timestampFormatter(): TimestampFormatter =
    new TimestampFormatter(task, Optional.absent())

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
          KeyToRedisOutputPlugin.redis.foreach(_.sadd(hash))
        } else {
          KeyToRedisOutputPlugin.redis.foreach(
            _.sadd(setValueVisitor.getValue))
        }
      }
    }
    reader.close()
  }

  override def finish(): Unit = ()
  override def close(): Unit = ()
  override def commit(): TaskReport = Exec.newTaskReport
  override def abort(): Unit = ()
}
