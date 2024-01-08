import scala.util.Random
import akka.actor._

class AverageMasterActor(size: Int, numWorkers: Int) extends Actor {
  private val workers = (1 to numWorkers).map(i => context.actorOf(Props[Worker],
    s"worker$i"))
  private val aggregator = context.actorOf(Props(new Aggregator(numWorkers)),
    "aggregator")
  private val fragmentSize: Int = size / numWorkers
  private val random = Random
  val array: Array[Long] = Array.fill(size)(random.nextInt(size))

  def receive: Receive = {
    case CalculateAverage() =>
      println(s"Calculate average for array size $size")
      for (i <- 0 until numWorkers) {
        workers(i) ! Process(array.slice(i * fragmentSize, (i + 1) * fragmentSize))
      }
    case WorkerResult(sum, count) =>
      println(s"Worker result is sum=$sum and count=$count")
      aggregator ! Aggregate(sum, count)
    case FinalizeResult(avg) =>
      println(s"Average: $avg")
      context.system.terminate()
  }
}

class Worker extends Actor {
  def receive: Receive = {
    case Process(fragment: Array[Long]) =>
      val sum = fragment.sum
      val count = fragment.length
      sender() ! WorkerResult(sum, count)
  }
}

class Aggregator(numWorkers: Int) extends Actor {
  private var numResults = 0L
  private var sum = 0L
  private var count = 0L

  def receive: Receive = {
    case Aggregate(sum, count) =>
      println(s"aggregate process")
      this.sum += sum
      this.count += count
      numResults += 1
      if (numResults == numWorkers) {
        val avg = sum / count
        sender() ! FinalizeResult(avg)
      }
  }
}

case class CalculateAverage()

case class FinalizeResult(avg: Double)

case class Process(fragment: Array[Long])

case class Aggregate(sum: Long, count: Long)

case class WorkerResult(sum: Long, count: Long)

object task extends App {
  println("Task 1")
  val n = 1_000_000
  val system = ActorSystem("AverageSystem")
  val actor = system.actorOf(Props(new AverageMasterActor(n, 2)), name =
    "averageMaster")
  actor ! CalculateAverage()
  println
}