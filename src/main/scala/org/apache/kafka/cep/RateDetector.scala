package org.apache.kafka.cep

import java.util.concurrent.TimeUnit
import org.apache.kafka.cep.{Aggregate, Mergable, ConcurrentSlidingWindow}
import java.util.concurrent.ConcurrentNavigableMap
import Moments

trait RateDetector extends Detector {

  //timeFrame * 2 is necessary because events come with varying latencies but that flattens the rate mean
  //so later we have to divide the sum by / 2 as well
  val window = new ConcurrentSlidingWindow[Double](timeFrame * 2, unit)

  final def mark(timestamp: Long) {
    val t = (timestamp - timeFrameMillis)
    window.add(t, 1)
    val dist = window.dist(t)
    if (!dist.mean.isNaN) {
      val eid = (math.ceil(t / timeFrameMillis).toLong * timeFrameMillis).toString
      getFutureEvent(eid, t, 1) match {
        case None => {}
        case Some(event) ⇒ {
          event.set(0, timestamp)
          update(event, dist.mean)
          update(event, "sum", window.sum(t) / 2)
          //we do not notifyObservers but wait for expiration of the future
        }
      }

    }
  }
}