package org.apache.kafka.cep.graphite

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Meter, MetricFilter, MetricRegistry, Timer}
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.kafka.cep.Config

class GraphiteMetrics(val config: Config) {
    val metrics = new MetricRegistry
    val graphite = new Graphite(new InetSocketAddress(
            config.getProperty("graphite.host", "localhost"),
            Integer.valueOf(config.getProperty("graphite.port", "2003"))))

    val graphiteReporter = GraphiteReporter.forRegistry(metrics)
        .prefixedWith(config.getProperty("graphite.prefix", "kafka-cep"))
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL)
        .build(graphite)

    graphiteReporter.start(config.getProperty("graphite.period", "60").toLong, TimeUnit.SECONDS)

    val timers = CacheBuilder.newBuilder.build(new CacheLoader[String, Timer] {
        override def load(name: String): Timer = {
            return metrics.timer(name)
        }
    })

    val meters = CacheBuilder.newBuilder.build(new CacheLoader[String, Meter] {
        override def load(name: String): Meter = {
            return metrics.meter(name)
        }
    })

}