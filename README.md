# spark-taxi
## questions
1. Which zones/boroughs have the most pickups overall?
2. What are the peak hours?
3. How are the trips distributed 
4. Why are people using the taxi? (possible segmentation)
5. What are the peak hours for long/short trips 
6. What are the top 3 pickup dropoff zones for long/short trips
7. How are the people paying for long/short trips
8. Can we explore ridesharing by grouping trips together?

## running
`sbt assembly`

if running on java 19+, add to VM options:

`--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED`