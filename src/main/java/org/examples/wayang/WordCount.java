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

package org.examples.wayang;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {

        /* Get a plan builder */
        WayangContext wayangContext = new WayangContext(new Configuration())

                /* Uncomment the platform you want to use */
//                .withPlugin(Java.basicPlugin());
                .withPlugin(Spark.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("WordCount")
                .withUdfJarOf(WordCount.class);

        /* Get the absolute path of the input file */
        Path path = Paths.get("src/main/resources/input/test.txt").toAbsolutePath();

        /* Start building the Wayang Plan */
        Collection<Tuple2<String, Integer>> wordcounts = planBuilder
                /* Use the following if you do not use collect as a sink */
//        planBuilder
                /* Read the text file */
                .readTextFile("file:" + path.toUri().getPath()).withName("Load file")

                /* Split each line by non-word characters */
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withName("Split words")

                /* Filter empty tokens */
                .filter(token -> !token.isEmpty())
                .withName("Filter empty words")

                /* Attach counter to each word */
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")

                /* Use the following if you just want to see the execution plan */
//                .build().explain(false);

                /* Execute the plan and collect the results */
                .collect();

        System.out.println(wordcounts);

        /* You can replace the collect sink with one of the following options */
                /* Write the results to a text file */
//                .writeTextFile("file:/Users/zoi/Work/WAYANG/wayang-examples/src/main/resources/output/wordcount.txt", tuple2 -> tuple2.getField0().toString()+":"+tuple2.getField1(), "job1");
                /* Print the results to the console */
//                .forEach(tuple2 -> System.out.println(tuple2.getField0().toString()+":"+tuple2.getField1()));

    }
}

