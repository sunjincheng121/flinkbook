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

package org.sunny.study.java.lambda;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * LamdaApp
 */
public class LambdaApp {
    public static void main(String[] args){
        new Thread(() -> System.out.println("Haha")).start();

        List<Integer> integers = Arrays.asList(4, 5, 6, 1, 2, 3, 7, 8, 8, 9, 10);

        List<Integer> evens = integers.stream().filter(i -> i % 2 == 0)
                                      .collect(Collectors.toList()); //过滤出偶数列表 [4,6,8,8,10]

        evens.forEach(d -> System.out.println(d));

        List<Integer> sortIntegers = integers.stream().sorted()
                                             .limit(5).collect(Collectors.toList());//排序并且提取出前5个元素 [1,2,3,4,5]

        sortIntegers.forEach(d -> System.out.println(d));


        int[] numbers = {2, 3, 5, 7, 11, 13};
        int sum = Arrays.stream(numbers).sum();

        System.out.println(sum);

     long sumValue =  Arrays.stream(numbers).mapToLong(
                e -> {
                    long temp = 0;
                    for(int i = 1; i <= e; i++) {
                        if(e % i == 0) {
                            temp += i;
                        }
                    }
                    return temp;
                }
        ).sum();

     System.out.println(sumValue);
    }
}
