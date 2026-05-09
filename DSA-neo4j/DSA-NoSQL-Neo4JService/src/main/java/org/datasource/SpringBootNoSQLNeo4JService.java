package org.datasource;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.logging.Logger;


/*
 * mvn spring-boot:run
 */
@SpringBootApplication (exclude={DataSourceAutoConfiguration.class} )
public class SpringBootNoSQLNeo4JService extends SpringBootServletInitializer
{
	private static Logger logger = Logger.getLogger(SpringBootNoSQLNeo4JService.class.getName());
	
	public static void main(String[] args) {
		logger.info("Loading ... SpringBootNoSQLNeo4JService Default Settings ... JSON");
		SpringApplication.run(SpringBootNoSQLNeo4JService.class, args);
	}
}

/*
> Neo4J Java Client
- Working with Neo4J Driver
	- https://neo4j.com/docs/java-manual/current/
	- https://www.baeldung.com/java-neo4j

> OGM (Object-Graph-Mapping) with Java Client
	- https://neo4j.com/docs/ogm-manual/current/
		- https://neo4j.com/docs/ogm-manual/current/tutorial/
	- https://www.baeldung.com/java-neo4j (6)

> Neo4j SpringBoot Client
- Neo4J Repositories (Neo4j Spring Data Mapping)
	- https://spring.io/guides/gs/accessing-data-neo4j
		- https://docs.spring.io/spring-data/mongodb/reference/mongodb/mapping/mapping.html
	- https://neo4j.com/docs/getting-started/languages-guides/java/spring-data-neo4j/
	- https://www.baeldung.com/spring-data-neo4j-intro
	- https://medium.com/globant/unlocking-the-potential-of-relationships-spring-boot-and-neo4j-integration-1b712471f3e4

*/