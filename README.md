# Wayang examples
Examples of simple Wayang jobs

1. Make sure you are using Java 17.

2. If you want to run a job with Spark, make sure that you use these Java VM options:

   --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED    --add-opens java.base/java.io=ALL-UNNAMED   --add-opens java.base/java.nio=ALL-UNNAMED    --add-opens java.base/java.lang=ALL-UNNAMED  --add-opens java.base/java.lang.invoke=ALL-UNNAMED   --add-opens java.base/sun.security.util=ALL-UNNAMED   --add-opens java.base/sun.security.action=ALL-UNNAMED
