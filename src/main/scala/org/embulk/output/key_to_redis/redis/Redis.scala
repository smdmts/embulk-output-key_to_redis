package org.embulk.output.key_to_redis.redis

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import redis.RedisClient

import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

case class Redis(setKey: String, host: String, port: Int, db: Option[Int]) {
  implicit val actorSystem = akka.actor.ActorSystem(
    "redis-client",
    classLoader = Some(this.getClass.getClassLoader))

  val redis = RedisClient(host, port, db = db)
  val sender: ActorRef = actorSystem.actorOf(Props(Sender(setKey, redis)))

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

  def sadd(value: String): Unit = {
    sender ! Message(value)
  }

  def flush(): Boolean = {
    Await.result(redis.flushdb(), 10.minute)
  }

  def close(): Unit = {
    sender ! Close
    implicit val timeout: Timeout = Timeout(1000.seconds)
    var finished = false
    while (!finished) {
      val f = sender ? GetStatus
      val result = Await.result(f.mapTo[Result], Duration.Inf)
      if (!result.finished) {
        Thread.sleep(1000)
      } else {
        finished = result.finished
      }
    }
    redis.stop()
    // wait for redis stop.
    Thread.sleep(1000)
    actorSystem.shutdown()
  }

}
