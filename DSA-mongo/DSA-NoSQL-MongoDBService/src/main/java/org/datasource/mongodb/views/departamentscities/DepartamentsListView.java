package org.datasource.mongodb.views.departamentscities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@JsonIgnoreProperties({"_id"})
@Data @AllArgsConstructor @NoArgsConstructor(force = true)
public class DepartamentsListView {
	private List<DepartamentView> departaments;
}

/*
{
    "_id" : ObjectId("64478d7e58d27f112436508f"),
    "departaments" : [
        {
            "idDepartament" : NumberInt(2),
            "departamentName" : "Neamt",
            "departamentCode" : "NT",
            "countryName" : "RO",
            "cities" : [
                {
                    "idCity" : NumberInt(3),
                    "cityName" : "Piatra Neamt",
                    "postalCode" : "2001"
                },
                {
                    "idCity" : NumberInt(4),
                    "cityName" : "Roman",
                    "postalCode" : "2002"
                }
            ]
        },
        {
            "idDepartament" : NumberInt(1),
            "departamentName" : "Iasi",
            "departamentCode" : "IS",
            "countryName" : "RO",
            "cities" : [
                {
                    "idCity" : NumberInt(1),
                    "cityName" : "Iasi",
                    "postalCode" : "1001"
                },
                {
                    "idCity" : NumberInt(2),
                    "cityName" : "Pascani",
                    "postalCode" : "1002"
                }
            ]
        }
    ]
}

 */