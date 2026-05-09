package org.datasource.jdbc.views.customerdetails;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
public class CustomerDetailsViewBuilder {
	// SQL Map
	private String SQL_CUSTOMERS_DETAILS_SELECT =
			"SELECT cust_id, credit_rating, industry, age, turnover, nr_of_emps, comp_type " +
			"FROM customers_details";

	// DataCache
	private List<CustomerDetailsView> customersDetailsViewList = new ArrayList<>();

	public List<CustomerDetailsView> getViewList() {
		// building workflow
		// this.build().filter().getViewList();
		return this.customersDetailsViewList;
	}

	// building steps
	public CustomerDetailsViewBuilder build() {
		try (Connection jdbcConnection = jdbcConnector.getConnection()) {
			// extract data
			Statement selectStmt = jdbcConnection.createStatement();
			ResultSet rs = selectStmt.executeQuery(SQL_CUSTOMERS_DETAILS_SELECT);

			// map data to EntityView
			customersDetailsViewList = new ArrayList<>();
			while (rs.next()) {
				Integer cust_id = rs.getInt("cust_id");
				String credit_rating = rs.getString("credit_rating");
				String industry = rs.getString("industry").trim();
				Integer age = rs.getInt("age");
				Double turnover = rs.getDouble("turnover");
				Double nr_of_emps = rs.getDouble("nr_of_emps");
				String comp_type = rs.getString("comp_type").trim();
				this.customersDetailsViewList.add(new CustomerDetailsView(cust_id, credit_rating, industry,
						age, turnover, nr_of_emps, comp_type));
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return this;
	}

	/* JDBC Session Management ---------------------------------------- */
	private JDBCDataSourceConnector jdbcConnector;
	public CustomerDetailsViewBuilder(JDBCDataSourceConnector jdbcConnector) {
		this.jdbcConnector = jdbcConnector;
	}
}
