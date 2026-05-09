package org.datasource;

import org.datasource.neo4j.views.locations.CityView;
import org.datasource.neo4j.views.locations.DepartamentView;
import org.datasource.neo4j.views.locations.DepartamentViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Logger;


/*	REST Service URL
	http://localhost:8094/DSA-NoSQL-Neo4JService/rest/locations/DepartamentView
	http://localhost:8094/DSA-NoSQL-Neo4JService/rest/locations/CityView
*/
@RestController @RequestMapping("/locations")
public class RESTViewServiceNeo4J {
	private static Logger logger = Logger.getLogger(RESTViewServiceNeo4J.class.getName());
	
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

	// Set-up 
	@Autowired private DepartamentViewBuilder viewBuilder;
}