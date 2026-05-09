package org.datasource.mongodb.views.departamentscities;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.datasource.mongodb.MongoDataSourceConnector;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class DepartamentViewBuilder {

	// Data cache
	private DepartamentsListView departamentsListView;
	private List<DepartamentView> departamentsViewList;
	private List<CityView> citiesViewList;
	
	public DepartamentsListView getDepartamentsListView() {
		return departamentsListView;
	}

	public List<DepartamentView> getDepartamentsViewList() {
		return departamentsViewList;
	}

	public List<CityView> getCitiesViewList() {
		return citiesViewList;
	}

	//
	private MongoDataSourceConnector dataSourceConnector;
	
	
	public DepartamentViewBuilder(MongoDataSourceConnector dataSourceConnector) {
		this.dataSourceConnector = dataSourceConnector;
	}

	// Builder Workflow
	public DepartamentViewBuilder build() throws Exception{
		return this.select().map();
	}
		
	private DepartamentViewBuilder map() {
		this.departamentsViewList = this.departamentsListView.getDepartaments();
		this.citiesViewList = new ArrayList<>();
		
		for(DepartamentView departamentView: departamentsViewList)
			this.citiesViewList.addAll(departamentView.getCities());

		return this;
	}
	
	public DepartamentViewBuilder select() throws Exception {
		MongoDatabase db = dataSourceConnector.getMongoDatabase();

		MongoCollection<DepartamentsListView> departamentsCollection =
				db.getCollection("DepartamentsCities", DepartamentsListView.class);
		this.departamentsListView = departamentsCollection.find().first();
		//
		departamentsListView.getDepartaments().forEach(System.out::println);
		//
		return this;
	}
}
