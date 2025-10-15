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
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;

import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.java.Java;
import org.apache.wayang.flink.Flink;
import org.apache.wayang.giraph.Giraph;

import java.util.Collection;

public class FinanceQuery {
    public static void main(String[] args){

        /* Define the context */
        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Flink.basicPlugin())
                .with(Postgres.plugin())
                .with(Giraph.plugin());


        /* Get a plan builder */
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("FinanceQuery")
                .withUdfJarOf(FinanceQuery.class);

        /* Start building the Apache Wayang Plan */
        Collection<Record> output = planBuilder
                /* Read the text file */
                .readTextFile("file:/Users/zoi/Work/wayang-test/src/main/resources/input/Stock_Prices_2022.csv")
                .map(line -> {
                    String [] vals = line.split("\\,");
                    return new Record(vals[0], vals [1], Double.parseDouble(vals[2]), 1);
                })
                /* Filter for year 2022 */
                .filter(record -> ((String) record.getField(0)).contains("2022"))

                /* Aggregate */
                .reduceByKey(record -> record.getField(1), (r1, r2) -> new Record(r1.getField(0), r1.getField(1), r1.getDouble(2) + r2.getDouble(2), r1.getInt(3) + r2.getInt(3)))
                /* Average */
                .map(r -> new Record(r.getField(1), r.getDouble(2)/r.getInt(3)))
                /* Uncomment the following line to force execution of this operator on Spark
                * For the rest of the operators the optimizer will figure out where it is best to execute them */
//                .withTargetPlatform(Spark.platform())
                .collect();

        /* Print out the results */
        System.out.println(output);
    }
}
