# lettuce cluster client demo
1. provision Redis Enterprise cluster
2. Update application.properties file for password and url of ACRE cluster (note format - redis://:<password>=@<URL>:<port>)
3. install libs and run 
4. watch logs or use redis cli to confirm key set with the clusterclient from lettuce