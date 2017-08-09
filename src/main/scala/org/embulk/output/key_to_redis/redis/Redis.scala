package org.embulk.output.key_to_redis.redis

import redis.RedisClient

import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

case class Redis(setKey: String, host: String, port: Int, db: Option[Int]) {
  implicit val actorSystem = akka.actor.ActorSystem(
    "redis-client",
    classLoader = Some(this.getClass.getClassLoader))
  val redis = RedisClient(host, port, db = db)

  def ping(): String = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val s: Future[String] = redis.ping()
    s.onComplete {
      case Success(result) => result
      case Failure(t) =>
        actorSystem.shutdown()
        throw t
    }
    Await.result(s, 10.minute)
  }
  def sadd(value: String): Long = {
    val s = redis.sadd(setKey, value)
    Await.result(s, 10.minute)
  }

  def flush(): Boolean = {
    Await.result(redis.flushdb(), 10.minute)
  }

  def close(): Unit = {
    redis.stop()
    // wait for stopping.
    Thread.sleep(1000)
    actorSystem.shutdown()
  }

}
