package org.datasource;

import org.datasource.csv.custcategories.CustomerCategoryView;
import org.datasource.csv.custcategories.CustomerEmpCategoryCSVViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Logger;


/*	REST Service URL
	http://localhost:8097/DSA-DOC-CSVService/rest/customers/CustomerEmployeesCategoryViewCSV
*/
@RestController @RequestMapping("/customers")
public class RESTViewServiceCSV {
	private static Logger logger = Logger.getLogger(RESTViewServiceCSV.class.getName());

	@RequestMapping(value = "/CustomerEmployeesCategoryViewCSV", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<CustomerCategoryView> get_CustomerEmployeesCategoryViewCSV() throws Exception {
		List<CustomerCategoryView> viewList;
		if (this.customerEmpCategoryCSVViewBuilder.getViewList().isEmpty() == true)
			viewList = this.customerEmpCategoryCSVViewBuilder.build().getViewList();
		else
			viewList = this.customerEmpCategoryCSVViewBuilder.getViewList();
		return viewList;
	}

	// Set-up
	@Autowired private CustomerEmpCategoryCSVViewBuilder customerEmpCategoryCSVViewBuilder;
}