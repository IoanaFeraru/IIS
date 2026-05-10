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
public class SpringBootNoSQLMongoDBService extends SpringBootServletInitializer
{
	private static Logger logger = Logger.getLogger(SpringBootNoSQLMongoDBService.class.getName());
	
	public static void main(String[] args) {
		logger.info("Loading ... SpringBootNoSQLMongoDBService Default Settings ... JSON");
		SpringApplication.run(SpringBootNoSQLMongoDBService.class, args);
	}
}