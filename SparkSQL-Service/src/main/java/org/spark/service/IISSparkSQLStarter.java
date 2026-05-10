package org.spark.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.logging.Logger;

/**
 * IIS SparkSQL Service — Spring Boot entry point.
 *
 * Excludes DataSourceAutoConfiguration because Spring Boot must NOT try to
 * auto-configure a JDBC DataSource; Spark manages its own session internally.
 *
 * JVM flags required at runtime (add to JAVA_TOOL_OPTIONS in Docker or IDE run config):
 *   --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 *   --add-opens   java.base/java.net=ALL-UNNAMED
 *   --add-opens   java.base/java.io=ALL-UNNAMED
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class IISSparkSQLStarter extends SpringBootServletInitializer {

    private static final Logger log = Logger.getLogger(IISSparkSQLStarter.class.getName());

    public static void main(String[] args) {
        log.info(">>> Starting IIS-SparkSQL-Service ...");
        SpringApplication.run(IISSparkSQLStarter.class, args);
    }
}
