package org.sunny.study.scala

import java.util.{ArrayList => JList}
import org.sunny.study.java.ref.RefInterface

class SCALAB(seq: Seq[String]) extends RefInterface {
  override def echo(): Unit = {
    seq.foreach(println(_))
  }

}
