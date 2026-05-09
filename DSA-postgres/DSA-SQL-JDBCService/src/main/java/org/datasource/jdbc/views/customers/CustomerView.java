package org.datasource.jdbc.views.customers;

import lombok.Value;

@Value
public class CustomerView {
	private Integer customerId;
	private String name;
	private String registrationcode;
}
