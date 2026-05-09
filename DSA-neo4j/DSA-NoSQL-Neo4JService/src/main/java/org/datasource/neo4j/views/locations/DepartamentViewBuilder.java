package org.datasource.neo4j.views.locations;

import org.datasource.neo4j.Neo4JDataSourceConnector;
import org.neo4j.ogm.session.Session;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class DepartamentViewBuilder {
	private static Logger logger = Logger.getLogger(DepartamentViewBuilder.class.getName());
	// Data cache
	private List<RegionView> regionsViewList;
	private List<DepartamentView> departamentsViewList;
	private List<CityView> citiesViewList;

	public List<DepartamentView> getDepartamentsViewList() {
		return departamentsViewList;
	}
	public List<CityView> getCitiesViewList() {
		return citiesViewList;
	}
	//
	private Neo4JDataSourceConnector dataSourceConnector;
	
	
	public DepartamentViewBuilder(Neo4JDataSourceConnector dataSourceConnector) {
		this.dataSourceConnector = dataSourceConnector;
	}

	// Builder Workflow
	public DepartamentViewBuilder build() throws Exception{
		return this.select();
	}

//	private DepartamentViewBuilder map() {}
	
	public DepartamentViewBuilder select() throws Exception {
		Session session = dataSourceConnector.getNeo4JSession();
		logger.info(">>> Building DepartamentView ... session open!");
		try{
			logger.info(">>> Building DepartamentView ... query RegionsView!");
			this.regionsViewList = new ArrayList<>(session.loadAll(RegionView.class));
			logger.info(">>> Building DepartamentView ... query DepartamentView!");
			this.departamentsViewList = new ArrayList<>(session.loadAll(DepartamentView.class));
			logger.info(">>> Building DepartamentView ... query CityView!");
			this.citiesViewList = new ArrayList<>(session.loadAll(CityView.class));
			//
			session.clear();
		} catch (Exception e) {
			session.clear();
			throw new RuntimeException(e);
		}
		return this;
	}
}
