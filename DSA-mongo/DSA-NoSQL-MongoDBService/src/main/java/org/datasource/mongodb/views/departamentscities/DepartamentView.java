package org.datasource.mongodb.views.departamentscities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@JsonIgnoreProperties("none")
@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class DepartamentView implements Serializable{
	private Long idDepartament;
	private String departamentName;
	private String departamentCode;
	private String countryName;

	private List<CityView> cities;
}


