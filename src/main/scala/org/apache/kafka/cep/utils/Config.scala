package org.apache.kafka.cep.utils

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConverters._

class Config(configFilePath: String) extends Properties {

  val configFile = new File(configFilePath)

  load(getClass getResourceAsStream configFilePath)

  if (configFile exists) {
    putAll(new Properties() {
      {
        load(new FileInputStream(configFile))
      }
    })
  }

  val config = this.asScala

  def apply(propertyName: String) = config(propertyName)

  def apply(propertyName: String, defaultValue: String) = {
    if (config contains propertyName) {
      config(propertyName)
    } else {
      defaultValue
    }
  }

}