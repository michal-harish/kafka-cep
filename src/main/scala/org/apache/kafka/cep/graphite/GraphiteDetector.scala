package org.apache.kafka.cep.graphite

import java.util.concurrent.TimeUnit

import grizzled.slf4j.Logger
import org.apache.kafka.cep._

class GraphiteSumDetector(targetTemplate: String, hosts: String*)(implicit system:CEP) 
    extends ValueAggregateDetector(hosts.map(host ⇒ new GraphiteDetector(targetTemplate.replace("<_>", host))): _*) 

class GraphiteDetector(val target: String)(implicit system:CEP) extends Detector(0, TimeUnit.SECONDS) {

  system.register(target)

  val log = Logger(classOf[GraphiteDetector])

  override def toString: String = name + "["+ target + "]"

  override def handle(observed: Observed, event: Any) = {
    event match {
      case g:GraphiteDataPoint if (g.target.equals(target)) => {
        notifyObservers(update(new Event((g.time * 1000).toString, g.time * 1000), g.value))
      }
      case _ => {}
    }
  }
}
