package org.datasource.jdbc.views.customersadresses;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
public class CustomerAddressesViewBuilder {
	// SQL Map
	private String SQL_CUSTOMERS_ADDRESSES_SELECT =
			"SELECT cust_id, address, city, country_code, postal_code, email, " +
					"social_provider, social_page_url, website_url " +
			"FROM customers_addresses";

	// DataCache
	private List<CustomerAddressesView> customersAddressesViewList = new ArrayList<>();

	public List<CustomerAddressesView> getViewList() {
		// building workflow
		// this.build().filter().getViewList();
		return this.customersAddressesViewList;
	}

	// building steps
	public CustomerAddressesViewBuilder build() {
		try (Connection jdbcConnection = jdbcConnector.getConnection()) {
			// extract data
			Statement selectStmt = jdbcConnection.createStatement();
			ResultSet rs = selectStmt.executeQuery(SQL_CUSTOMERS_ADDRESSES_SELECT);

			// map data to EntityView
			customersAddressesViewList = new ArrayList<>();
			while (rs.next()) {
				Integer cust_id = rs.getInt("cust_id");
				String address = rs.getString("address");
				String city = rs.getString("city").trim();
				String country_code = rs.getString("country_code");
				String postal_code = rs.getString("postal_code");
				String email = rs.getString("email");
				String social_provider = rs.getString("social_provider");
				String social_page_url = rs.getString("social_page_url");
				String website_url = rs.getString("website_url");

				this.customersAddressesViewList.add(new CustomerAddressesView(cust_id, address, city,
						country_code, postal_code, email, social_provider, social_page_url, website_url));
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return this;
	}

	/* JDBC Session Management ---------------------------------------- */
	private JDBCDataSourceConnector jdbcConnector;

	public CustomerAddressesViewBuilder(JDBCDataSourceConnector jdbcConnector) {
		this.jdbcConnector = jdbcConnector;
	}

}
