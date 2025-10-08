package org.examples.wayang;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.MapDataQuantaBuilder;
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

        // Read customers.csv from PostgreSQL
        DataQuantaBuilder<?, Record> transactions =
                planBuilder.readTable(new PostgresTableSource("transactions"))
                        .filter(tuple -> (Double) tuple.getField(2) > 1000);

//        System.out.println("Customers:" + customers.csv.collect());

        // Read transactions from HDFS/Spark file
        DataQuantaBuilder<?, String> customersFile = planBuilder.readTextFile("file:///Users/zoi/Work/wayang-test/src/main/resources/input/customers.csv");

        // Map transactions to Record(customerId, name, location)
        MapDataQuantaBuilder<?, Record> customers = customersFile.map(line -> {
            String[] cols = line.split(",");
            return new Record(Integer.parseInt(cols[0]), cols[1], cols[2]);
            });

//        System.out.println("Transactions:" + transactions.collect());

        // Join customers.csv with transactions on customerId
        Collection<Tuple2<Record, Record>> joined = customers
//                .map(record -> new Record(record.getInt(0), record.getField(1))) // id, name
                .join(  customerRecord -> customerRecord.getInt(0), // customer.id
                        transactions,
                        transactionsRecord -> transactionsRecord.getInt(1) // transaction.customerId
                )
                // Filter high-value transactions (>1000)
                // Map to churn score (dummy)
//                .map(tuple -> new Tuple2<>(tuple.field0.getInt(0), Math.random()))
                .collect();

        // Print output
        for (Tuple2<Record, Record> t : joined) {
            System.out.println(t);
        }
    }
}
