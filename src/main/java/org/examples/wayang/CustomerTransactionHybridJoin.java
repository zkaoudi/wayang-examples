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

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;

import java.util.Collection;

public class CustomerTransactionHybridJoin {

    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/mydb");
        configuration.setProperty("wayang.postgres.jdbc.user", "zoi");

        // Create Wayang context
        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        // Plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("CustomerTransactionChurn")
                .withUdfJarOf(CustomerTransactionHybridJoin.class);

        // Read transactions from PostgreSQL
        DataQuantaBuilder<?, Record> transactions =
                planBuilder.readTable(new PostgresTableSource("transactions"))
                        .filter(tuple -> (Double) tuple.getField(2) > 1000);


        DataQuantaBuilder<?, Record> customers = planBuilder
                // Read customers from csv file
                .readTextFile("file:///Users/zoi/Work/wayang-test/src/main/resources/input/customers.csv")

        // Map customers to Record(customerId, name, location)
                .map(line -> {
                    String[] cols = line.split(",");
                        return new Record(Integer.parseInt(cols[0]), cols[1], cols[2]);
                });


        // Join customers with transactions on customerId
        Collection<Tuple2<Record, Record>> joined = customers
                .join(  customerRecord -> customerRecord.getInt(0), // customer.id
                        transactions,
                        transactionsRecord -> transactionsRecord.getInt(1) // transaction.customerId
                )
                .collect();

        // Print output
        for (Tuple2<Record, Record> t : joined) {
            System.out.println(t);
        }
    }
}
