package org.embulk.output.key_to_redis

import java.util

import org.embulk.config._
import org.embulk.output.key_to_redis.redis.Redis
import org.embulk.spi._

class KeyToRedisOutputPlugin extends OutputPlugin {

  override def transaction(config: ConfigSource,
                           schema: Schema,
                           taskCount: Int,
                           control: OutputPlugin.Control): ConfigDiff = {
    val task = config.loadConfig(classOf[PluginTask])
    KeyToRedisOutputPlugin.createRedisInstance(task)
    KeyToRedisOutputPlugin.redis.foreach(_.ping())
    KeyToRedisOutputPlugin.taskCountOpt = Some(taskCount)
    if (task.getFlushOnStart) {
      KeyToRedisOutputPlugin.redis.foreach(_.flush())
    } else {
      if (task.getDeleteKeyOnStart) {
        KeyToRedisOutputPlugin.redis.foreach(_.deleteKey())
      }
    }
    control.run(task.dump())
    KeyToRedisOutputPlugin.redis.foreach(_.close())
    Exec.newConfigDiff
  }

  override def resume(taskSource: TaskSource,
                      schema: Schema,
                      taskCount: Int,
                      control: OutputPlugin.Control): ConfigDiff =
    throw new UnsupportedOperationException(
      "key to redis output plugin does not support resuming")

  override def cleanup(taskSource: TaskSource,
                       schema: Schema,
                       taskCount: Int,
                       successTaskReports: util.List[TaskReport]): Unit = {}

  override def open(taskSource: TaskSource,
                    schema: Schema,
                    taskIndex: Int): TransactionalPageOutput = {
    val task = taskSource.loadTask(classOf[PluginTask])
    KeyToRedisOutputPlugin.redis match {
      case Some(_) => // nothing to do
      case None => // for map reduce executor.
        KeyToRedisOutputPlugin.createRedisInstance(task)
    }
    PageOutput(taskSource,
               schema,
               KeyToRedisOutputPlugin.taskCountOpt,
               task.getPutAsMD5)
  }

}

object KeyToRedisOutputPlugin {
  var redis: Option[Redis] = None
  var taskCountOpt: Option[Int] = None

  def createRedisInstance(task: PluginTask): Unit = {
    KeyToRedisOutputPlugin.redis = Some(
      Redis(task.getRedisSetKey, task.getHost, task.getPort, {
        if (task.getDb.isPresent) Some(task.getDb.get())
        else None
      }))
  }
}
