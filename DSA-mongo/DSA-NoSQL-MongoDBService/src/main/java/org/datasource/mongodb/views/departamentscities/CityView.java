package org.datasource.mongodb.views.departamentscities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class CityView implements Serializable{
	private Long idCity;
	private String cityName;
	private String postalCode;
}