package org.embulk.output.key_to_redis.redis

import akka.actor.{Actor, ActorSystem}
import redis.RedisClient

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

case class Sender(setKey: String, redis: RedisClient) extends Actor {

  implicit val actorSystem: ActorSystem = context.system
  val buffer = new ListBuffer[String]
  val command = new ListBuffer[Future[Long]]

  override def receive: Receive = {
    case Message(v) =>
      buffer.append(v)
      // bulk insert
      if (buffer.size == 10000) {
        command.append(redis.sadd(setKey, buffer: _*))
        buffer.clear()
      }
    case Close =>
      command.append(redis.sadd(setKey, buffer: _*))
      buffer.clear()
    case GetStatus =>
      sender() ! Result(command.forall(_.isCompleted))
  }
}

case object GetStatus
case class Result(finished: Boolean) extends AnyVal
case object Close
case class Message(v: String) extends AnyVal
