# Kafka Complex Event Processor

First of all, this thing isn't working yet but the framework was derived from a functioning variant.
The general idea for application is to:

1) source primary events from Kafka (08) topics and have detectors transforming them to CEP Event(s)
2) define higher order detectors(composite, series,...) with complex logic in pure scala

The framework's responsibility is to hide the fact that the primary events are distributed as well as
exploit Kafka to provide the mechanism for distributing and parallelising higher order events detection.

