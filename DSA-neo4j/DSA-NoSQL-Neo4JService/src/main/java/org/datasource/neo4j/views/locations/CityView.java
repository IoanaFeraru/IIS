package org.datasource.neo4j.views.locations;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;

import java.io.Serializable;

@NodeEntity( label = "City")
@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class CityView implements Serializable{
	@Id
	private Long idCity;
	private String cityName;
	private String postalCode;
}

