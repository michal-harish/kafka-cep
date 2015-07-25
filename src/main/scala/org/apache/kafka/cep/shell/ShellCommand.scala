package org.apache.kafka.cep.shell

import java.io.IOException

import scala.io.Source

trait ShellCommand[T] {

  def getShortHelp: String;

  def printDetailedHelp = println(getShortHelp)

  var shell: Shell[T] = null

  var context: T = _

  def registerContext(shellInstance: Shell[T], contextInstance: T) = {
    this.shell = shellInstance
    this.context = contextInstance
  }

  @throws(classOf[IOException])
  def invoke(stdin: Source, args: String)

}