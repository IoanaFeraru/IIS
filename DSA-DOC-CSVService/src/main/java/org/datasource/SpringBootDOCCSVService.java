package org.datasource;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.logging.Logger;


/*
 * java -jar DSA-DOC-CSVService-2025.1.jar --xlsx.data.source.file.path=/home/catalin/data/CTG_CUST_EMP.csv
 */
@SpringBootApplication (exclude={DataSourceAutoConfiguration.class} )
public class SpringBootDOCCSVService extends SpringBootServletInitializer {
	private static Logger logger = Logger.getLogger(SpringBootDOCCSVService.class.getName());
	
	public static void main(String[] args) {
		logger.info("Loading ... SpringBootDOCCSVService Default Settings ... CSV");
		SpringApplication.run(SpringBootDOCCSVService.class, args);
	}
}