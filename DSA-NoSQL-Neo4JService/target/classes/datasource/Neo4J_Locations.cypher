// Script curatare -------------------------------------//
MATCH (n1)-[r]-(n2) DELETE r;
MATCH (n) DETACH DELETE n;

// Script CREARE ---------------------------------------//
CREATE (Moldova:Region { idRegion : 1, regionName : "Moldova", regionLoc : "NE"})
CREATE (VS:Departament { idDepartament : 3, departamentName : "Vaslui", departamentCode : "VS", countryName : "RO"})
CREATE (SV:Departament { idDepartament : 4, departamentName : "Suceava", departamentCode : "SV", countryName : "RO"})
CREATE (BC:Departament { idDepartament : 5, departamentName : "Bacau", departamentCode : "BC", countryName : "RO"})

CREATE (Vaslui:City {idCity : 5, cityName : "Vaslui", postalCode : "3001"})
CREATE (Barlad:City {idCity : 6, cityName : "Barlad", postalCode : "3002"})

CREATE (Suceava:City {idCity : 7, cityName : "Suceava", postalCode : "4001"})
CREATE (Falticeni:City {idCity : 8, cityName : "Falticeni", postalCode : "4002"})

CREATE (Bacau:City {idCity : 9, cityName : "Bacau", postalCode : "5001"})
CREATE (Onesti:City {idCity : 10, cityName : "Onesti", postalCode : "5002"})

CREATE
    (VS)-[:PART_OF]->(Moldova),
    (SV)-[:PART_OF]->(Moldova),
    (BC)-[:PART_OF]->(Moldova),
    (Vaslui)-[:LOCATED_IN]->(VS),
    (Barlad)-[:LOCATED_IN]->(VS),
    (Suceava)-[:LOCATED_IN]->(SV),
    (Falticeni)-[:LOCATED_IN]->(SV),  
    (Bacau)-[:LOCATED_IN]->(BC),
    (Onesti)-[:LOCATED_IN]->(BC);


// Script Interogare  ---------------------------------------//    
MATCH (city:City) RETURN *;
MATCH (departament:Departament) RETURN *;
MATCH (n1)-[r]-(n2) RETURN r;

MATCH (city:City) -[r:LOCATED_IN]-> (departament:Departament) 
RETURN city.idCity, departament.idDepartament;

MATCH (city:City) -[r:LOCATED_IN]-> (departament:Departament) 
RETURN city.idCity, city.cityName, city.postalCode, departament.idDepartament, 
departament.departamentName, departament.countryName;