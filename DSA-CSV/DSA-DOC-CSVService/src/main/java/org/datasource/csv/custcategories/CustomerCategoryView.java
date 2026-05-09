package org.datasource.csv.custcategories;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class CustomerCategoryView {
	private String categoryCode;
	private String categoryName;
	private Double lowerBound;
	private Double upperBound;
}
