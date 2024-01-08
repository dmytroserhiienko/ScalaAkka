package scala

import scala.actors.Actor

case object Ping
case object Pong
case object Stop

class Ping(count: Int, pong: Actor) extends Actor {
  def act() {
    var pingsLeft = count - 1
    pong ! Ping
    loop {
      react {
        case Pong =>
          if (pingsLeft % 1000 == 0)
            Console.println("pingpong.Ping: pong")
          if (pingsLeft > 0) {
            pong ! Ping
            pingsLeft -= 1
          } else {
            Console.println("pingpong.Ping: stop")
            pong ! Stop
            exit()
          }
      }
    }
  }
}

class Pong extends Actor {
  def act() {
    var pongCount = 0
    loop {
      react {
        case Ping =>
          if (pongCount % 1000 == 0)
            Console.println("pingpong.Pong: ping " + pongCount)
          sender ! Pong
          pongCount = pongCount + 1
        case Stop =>
          Console.println("pingpong.Pong: stop")
          exit()
      }
    }
  }
}

object pingpong extends App {
  val pong = new Pong
  val ping = new Ping(100000, pong)
  ping.start
  pong.start
}
