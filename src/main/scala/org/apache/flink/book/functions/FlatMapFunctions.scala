/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.book.functions

import java.util.StringTokenizer

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.book.utils.DataSetUtils.Words
import org.apache.flink.util.Collector

object FlatMapFunctions {
  class TokenizerFlatMap2 extends FlatMapFunction[String, String] {
    override def flatMap(
      t: String,
      out: Collector[String]): Unit = {
      val stringTokenizer = new StringTokenizer(t, ",!' .;", false)
      while(stringTokenizer.hasMoreElements()) {
        val word = stringTokenizer.nextToken.trim.toLowerCase
        out.collect(word)
      }
    }
  }
  class WordsTokenizerFlatMap extends FlatMapFunction[Words, String] {
    override def flatMap(
      t: Words,
      out: Collector[String]): Unit = {
      val stringTokenizer = new StringTokenizer(t.data, ",!' .;", false)
      while(stringTokenizer.hasMoreElements()) {
        val word = stringTokenizer.nextToken.trim.toLowerCase
        out.collect(word)
      }
    }
  }
}
