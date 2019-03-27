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

package org.sunny.study.java.ref;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * RefT
 */
public class RefT {
    public static void main(String[] args) throws Exception{
        Class clazz = Class.forName("org.sunny.study.scala.SCALAA");
        RefInterface ref = (RefInterface)clazz.newInstance();
        ref.echo();


        Class clazz2 = Class.forName("org.sunny.study.scala.SCALAB");
        Constructor[] cs = clazz2.getConstructors();
        ArrayList<String> a = new ArrayList<>();
        a.add("xx");



        RefInterface ref2 = (RefInterface)cs[0].newInstance(a);
        ref2.echo();

    }
}
