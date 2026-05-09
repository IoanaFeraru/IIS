package org.datasource;

import org.datasource.jdbc.JDBCDataSourceConnector;
import org.datasource.jdbc.views.customerdetails.CustomerDetailsView;
import org.datasource.jdbc.views.customerdetails.CustomerDetailsViewBuilder;
import org.datasource.jdbc.views.customers.CustomerView;
import org.datasource.jdbc.views.customers.CustomerViewBuilder;
import org.datasource.jdbc.views.customersadresses.CustomerAddressesView;
import org.datasource.jdbc.views.customersadresses.CustomerAddressesViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.logging.Logger;

/*	REST Service URL
 	http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView
 	http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerDetailsView
 	http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerAddressesView
*/
@RestController
@RequestMapping("/customers")
public class RESTViewServiceJDBC {
	private static Logger logger = Logger.getLogger(RESTViewServiceJDBC.class.getName());
	
	@RequestMapping(value = "/ping", method = RequestMethod.GET,
			produces = {MediaType.TEXT_PLAIN_VALUE})
	@ResponseBody
	public String ping() {
		logger.info(">>>> DSA-SQL-JDBCService:: RESTViewService is Up!");
		return "Ping response from DSA-SQL-JDBCService!";
	}
	
	@RequestMapping(value = "/CustomerView", method = RequestMethod.GET, 
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<CustomerView> get_CustomerView() {
		List<CustomerView> viewList = customersViewBuilder.build().getViewList();
		return viewList;
	}

	@RequestMapping(value = "/CustomerViewData", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<CustomerView> get_CustomerView(
			@RequestParam("fetch_offset") Integer fetchOffset,
			@RequestParam("fetch_size") Integer fetchSize
			) {
		List<CustomerView> viewList = customersViewBuilder
				.setFetchOffset(fetchOffset)
				.setFetchSize(fetchSize)
				.build().getViewList();
		return viewList;
	}

	@RequestMapping(value = "/CustomerDetailsView", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<CustomerDetailsView> get_CustomerDetailsView() {
		List<CustomerDetailsView> viewList = customersDetailsViewBuilder.build().getViewList();
		return viewList;
	}

	@RequestMapping(value = "/CustomerAddressesView", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<CustomerAddressesView> get_CustomerAddressesView() {
		List<CustomerAddressesView> viewList = customersAddressesViewBuilder.build().getViewList();
		return viewList;
	}
	// Set-up
	@Autowired private JDBCDataSourceConnector jdbcConnector;
	//
	@Autowired private CustomerViewBuilder customersViewBuilder;
	@Autowired private CustomerDetailsViewBuilder customersDetailsViewBuilder;
	@Autowired private CustomerAddressesViewBuilder customersAddressesViewBuilder;
	//
}