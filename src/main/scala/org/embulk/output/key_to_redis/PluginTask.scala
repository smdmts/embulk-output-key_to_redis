package org.embulk.output.key_to_redis

import com.google.common.base.Optional
import org.embulk.config.{Config, ConfigDefault, Task}
import org.embulk.spi.time.TimestampFormatter

trait PluginTask extends Task with TimestampFormatter.Task {

  @Config("redis_set_key")
  def getRedisSetKey: String

  @Config("flush_on_start")
  @ConfigDefault("false")
  def getFlushOnStart: Boolean

  @Config("delete_key_on_start")
  @ConfigDefault("false")
  def getDeleteKeyOnStart: Boolean

  @Config("put_as_md5")
  @ConfigDefault("false")
  def getPutAsMD5: Boolean

  @Config("key_with_index")
  @ConfigDefault("{}")
  def getKeyWithIndex: java.util.Map[String, String]

  @Config("json_key_with_index")
  @ConfigDefault("{}")
  def getJsonKeyWithIndex: java.util.Map[String, String]

  @Config("appender")
  @ConfigDefault("\"-\"")
  def getAppender: String

  @Config("host")
  @ConfigDefault("\"127.0.0.1\"")
  def getHost: String

  @Config("port")
  @ConfigDefault("6379")
  def getPort: Int

  @Config("db")
  @ConfigDefault("null")
  def getDb: Optional[Int]

}
