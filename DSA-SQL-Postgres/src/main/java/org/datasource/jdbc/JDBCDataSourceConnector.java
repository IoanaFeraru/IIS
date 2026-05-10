package org.datasource.jdbc;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.logging.Logger;

@Service
public class JDBCDataSourceConnector {
	private static Logger logger = Logger.getLogger(JDBCDataSourceConnector.class.getName());
			
	/* JDBC Session Management ---------------------------------------- */
	// JDBC driver name and database URL
	private String JDBC_DRIVER;
	@Value("${jdbc.data.source.DB_URL}")
	private String DB_URL;
	// Database credentials
	@Value("${jdbc.data.source.USER}")
	private String USER;
	@Value("${jdbc.data.source.PASS}")
	private String PASS;
	
	public Connection getConnection() throws Exception {
		// Class.forName(JDBC_DRIVER);
		Properties props = new Properties();
		props.setProperty("user", USER);
		props.setProperty("password", PASS);

		Connection conn = DriverManager.getConnection(DB_URL, props);
		return conn;
	}
}
