package org.datasource.neo4j.views.locations;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@NodeEntity( label = "Departament")
@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class DepartamentView implements Serializable{
	@Id
	private Long idDepartament;
	private String departamentName;
	private String departamentCode;
	private String countryName;

	@Relationship(type = "LOCATED_IN", direction = Relationship.Direction.UNDIRECTED)
	private List<CityView> cities = new ArrayList<>();
}


