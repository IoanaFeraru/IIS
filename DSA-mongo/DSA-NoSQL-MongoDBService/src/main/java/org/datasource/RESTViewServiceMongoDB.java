package org.datasource;

import org.datasource.mongodb.views.departamentscities.CityView;
import org.datasource.mongodb.views.departamentscities.DepartamentView;
import org.datasource.mongodb.views.departamentscities.DepartamentViewBuilder;
import org.datasource.mongodb.views.departamentscities.DepartamentsListView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Logger;


/*	REST Service URL
	http://localhost:8093/DSA-NoSQL-MongoDBService/rest/locations/DepartamentView
	http://localhost:8093/DSA-NoSQL-MongoDBService/rest/locations/CityView
	http://localhost:8093/DSA-NoSQL-MongoDBService/rest/locations/DepartamentsListView
*/
@RestController @RequestMapping("/locations")
public class RESTViewServiceMongoDB {
	private static Logger logger = Logger.getLogger(RESTViewServiceMongoDB.class.getName());
	
	@RequestMapping(value = "/ping", method = RequestMethod.GET,
		produces = {MediaType.TEXT_PLAIN_VALUE})
	@ResponseBody
	public String pingDataSource() {
		logger.info(">>>> org.datasource.rest.RESTViewService(JSON) is Up!");
		return "Ping response from RESTViewServiceMongoDB!";
	}
	
	@RequestMapping(value = "/DepartamentView", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<DepartamentView> get_DepartamentView() throws Exception {
		List<DepartamentView> viewList = this.viewBuilder.build().getDepartamentsViewList();
		return viewList;
	}
	
	@RequestMapping(value = "/CityView", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<CityView> get_CityView() throws Exception {
		List<CityView> viewList = this.viewBuilder.build().getCitiesViewList();
		return viewList;
	}
	
	@RequestMapping(value = "/DepartamentsListView", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public DepartamentsListView get_DepartamentsListView() throws Exception {
		DepartamentsListView dataView = this.viewBuilder.build().getDepartamentsListView();
		return dataView;
	}

	// Set-up 
	@Autowired private DepartamentViewBuilder viewBuilder;
}