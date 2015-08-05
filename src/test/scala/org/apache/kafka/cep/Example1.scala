package org.apache.kafka.cep

import java.util.concurrent.{TimeUnit, ScheduledThreadPoolExecutor}

import org.apache.kafka.cep.utils._

import scala.util.Random

/**
 * Created by mharis on 04/08/15.
 */

object Example1App extends App {
  new Example1("A")
    //.resetZooKeeperOffsets
    .startWithShell
}

class Example1(val id: String) extends CEP(Config.fromResource("/config.properties")) {

  val executor = new ScheduledThreadPoolExecutor(5)
  executor.scheduleWithFixedDelay(RandomByteGenerator, 1, 2, TimeUnit.SECONDS)

  object RandomByteGenerator extends Runnable {
    val r = new Random

    override def run: Unit = {
      processImpulse((r.nextInt.toByte, System.currentTimeMillis))
    }
  }

  val X = new Detector(10, TimeUnit.SECONDS) {
    //with RateDetector {
    override def handle(observed: Observed, event: Any) = event match {
      case (b:Byte, ts:Long) => {
        //mark(ts)
        println((observed.getClass.getSimpleName + " >> " + (b & 0xFF)))
        //println(observed.getClass.getSimpleName + ": " + )
      }
      case _ => {
        println("!" + event)
      }
    }
  }

}
