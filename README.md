# Wayang examples
Examples of simple Wayang jobs

## Installation notes

1. Make sure you are using Java 17.

2. If you want to run a job with Spark, make sure that you use these Java VM options:

   --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED    --add-opens java.base/java.io=ALL-UNNAMED   --add-opens java.base/java.nio=ALL-UNNAMED    --add-opens java.base/java.lang=ALL-UNNAMED  --add-opens java.base/java.lang.invoke=ALL-UNNAMED   --add-opens java.base/sun.security.util=ALL-UNNAMED   --add-opens java.base/sun.security.action=ALL-UNNAMED

This is required to ensure Spark and Java 17 compatibility.

## Examples

### Wordcount example
Counts the number of words in a text file.

### Finance example
Reads stock data from a CSV file and finds the average price of stocks for 2022

### Customer-Transactions Hybrid Join example
Joins customers data stored in a CSV file with transactions data stored in a Postgres table.

Note: If you want to run the `CustomerTransactionHybridJoin` job make sure you load into a PostgreSQL server the right data into a `transactions` table. See [this file](./src/main/resources/load2database).
