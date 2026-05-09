package org.datasource.jdbc.views.customers;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class CustomerViewBuilder {
	private static Logger logger = Logger.getLogger(CustomerViewBuilder.class.getName());
	// SQL Map
	private String SQL_CUSTOMERS_SELECT = "SELECT cust_id, name, registration_code FROM customers";
	private Integer fetchOffset = -1;
	private Integer fetchSize = 25;

	// DataCache
	private List<CustomerView> customersViewList = new ArrayList<>();

	public List<CustomerView> getViewList() {
		// building workflow
		// this.build().filter().getViewList();
		return this.customersViewList;
	}

	// building steps
	public CustomerViewBuilder build() {
		logger.info(">>> Building CustomerView: fetchOffset=" + fetchOffset + ", fetchSize=" + fetchSize + "");
		try (Connection jdbcConnection = jdbcConnector.getConnection()) {
			String sql = SQL_CUSTOMERS_SELECT;
			PreparedStatement selectStmt;
			// Prepare fetch SQL
			if (fetchOffset != null && fetchOffset > 0) {
				sql = String.format(SQL_FETCH_SELECT, SQL_CUSTOMERS_SELECT);
				selectStmt = jdbcConnection.prepareStatement(sql);
				logger.info(">>> SQL_FETCH_SELECT formatted:\n" + sql);
				selectStmt = jdbcConnection.prepareStatement(sql);
				selectStmt.setInt(1, fetchOffset);
				selectStmt.setInt(2, fetchOffset + fetchSize);
			}else
				selectStmt = jdbcConnection.prepareStatement(sql);
			// extract data
			ResultSet rs = selectStmt.executeQuery();
			// map data to EntityView
			customersViewList = new ArrayList<>();
			while (rs.next()) {
				Integer cust_id = rs.getInt("cust_id");
				String name = rs.getString("name");
				String registration_code = rs.getString("registration_code").trim();
				this.customersViewList.add(new CustomerView(cust_id, name, registration_code));
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return this;
	}

	/* JDBC Session Management ---------------------------------------- */
	private JDBCDataSourceConnector jdbcConnector;

	public CustomerViewBuilder(JDBCDataSourceConnector jdbcConnector) {
		this.jdbcConnector = jdbcConnector;
	}

	public CustomerViewBuilder setFetchOffset(Integer fetchOffset) {
		if (fetchOffset != null) {
			this.fetchOffset = fetchOffset;
		}
		return this;
	}
	public CustomerViewBuilder setFetchSize(Integer fetchSize) {
		if (fetchSize != null) {
			this.fetchSize = fetchSize;
		}
		return this;
	}

	private String SQL_FETCH_SELECT = """
			SELECT *
			  FROM (
			       SELECT Q_.*,
			              ROW_NUMBER() OVER(
			                      ORDER BY 1
			              ) RN___
			         FROM (
			              %s
			       ) Q_
			) Q__
			 WHERE RN___ BETWEEN ? AND ?
			""";
}