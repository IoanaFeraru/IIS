# IIS

## SQL REST Service: JDBC-DATA-SOURCE-SERVICE

### JDBC Spring Boot Service
* JDBC Drivers: ojdbc10, postgresql42, mysql-connector-j8
* SpringBoot 3

### SpringBoot service desc
* SBT jar: DSA-SQL-JDBCService-2026.1.jar

## Spring Boot Container Doc
* [Docker for SBT](https://spring.io/guides/gs/spring-boot-docker)
  
## DevOp Local Flow
* Image name: iis-dsa/DSA-SQL-JDBCService  
mvn package
docker build --build-arg JAR_NAME="DSA-SQL-JDBCService-2026.1.jar" -t iis-dsa/DSA-SQL-JDBCService .
docker run --name=DSA-SQL-JDBCService -p 8090:8090 -d iis-dsa/DSA-SQL-JDBCService  
docker start jdbc-data-source-service-wrapper 

## Clean-up docker
cd DSA-SQL-JDBCService
docker stop DSA-SQL-JDBCService
docker rm DSA-SQL-JDBCService
docker rmi iis-dsa/DSA-SQL-JDBCService
