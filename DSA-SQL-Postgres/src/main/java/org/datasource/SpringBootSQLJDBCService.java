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
public class SpringBootSQLJDBCService
		extends SpringBootServletInitializer
{
	private static Logger logger = Logger.getLogger(SpringBootSQLJDBCService.class.getName());
	
	public static void main(String[] args) {
		logger.info("Loading ... SpringBootDataService Default Settings ... .");
		SpringApplication.run(SpringBootSQLJDBCService.class, args);
	}
}


/*
	@Bean
	public Jackson2ObjectMapperBuilderCustomizer jacksonCustomizer() {
	    return (mapperBuilder) -> mapperBuilder.modulesToInstall(new JaxbAnnotationModule());
	}


# Script to manage 18c
# Start docker
open -a docker
# Start container
docker start 94277f0dc1ed
# Stop container
docker stop 94277f0dc1ed
# Stop docker
osascript -e 'quit app "Docker"'

open -a docker
# Start container
docker start oracle-xe-18c
# Stop container
docker stop oracle-xe-18c
# Stop docker
osascript -e 'quit app "Docker"'
*/

/*
# Script to manage PostgreSQL
# Start docker
open -a docker
# Start container
docker start postgresql-container
# Stop container
docker stop postgresql-container
# Stop docker
osascript -e 'quit app "Docker"'
*/

/*
sudo -i
VBoxManage startvm "Oracle DB Developer VM" --type headless
###
VBoxManage controlvm "Oracle DB Developer VM" savestate
###
*/