----------------------------------------------------------------------------------
--- DS6_Neo4J_SparkSQL_Views.sql
----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-NoSQL-Neo4JService/rest/locations/DepartamentView');

SELECT schema_of_json('[
	{"idDepartament":3,"departamentName":"Vaslui","departamentCode":"VS","countryName":"RO",
		"cities":[{"idCity":5,"cityName":"Vaslui","postalCode":"3001"}]}
    ]');

-- 2. Create Remote View
-- DROP VIEW departaments_view;
CREATE OR REPLACE VIEW departaments_view AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<cities: ARRAY<STRUCT<cityName: STRING, idCity: BIGINT, postalCode: STRING>>, 
					  countryName: STRING, departamentCode: STRING, departamentName: STRING, idDepartament: BIGINT>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8094/DSA-NoSQL-Neo4JService/rest/locations/DepartamentView')
        as data) json_raw
)
select v.idDepartament, v.departamentName, v.departamentCode, v.countryName
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM departaments_view;

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8094/DSA-NoSQL-Neo4JService/rest/locations/CityView');

SELECT schema_of_json('[{"idCity":3,"cityName":"Piatra Neamt","postalCode":"2001"}]');

-- 2. Create Remote View
-- DROP VIEW cities_view;
CREATE OR REPLACE VIEW cities_view AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<cityName: STRING, idCity: BIGINT, postalCode: STRING>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8094/DSA-NoSQL-Neo4JService/rest/locations/CityView')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM cities_view;


-- 1. Get Data Source JSON Schema
-- 2. Create Remote View
-- DROP VIEW departaments_cities_view;
CREATE OR REPLACE VIEW departaments_cities_view AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<cities: ARRAY<STRUCT<cityName: STRING, idCity: BIGINT, postalCode: STRING>>, 
					  countryName: STRING, departamentCode: STRING, departamentName: STRING, idDepartament: BIGINT>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8094/DSA-NoSQL-Neo4JService/rest/locations/DepartamentView')
        as data) json_raw
)
select v.idDepartament, explode(v.cities.idCity) as idCity
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM departaments_cities_view;
-- DROP VIEW departaments_cities_view_all;
CREATE OR REPLACE VIEW departaments_cities_view_all AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<cities: ARRAY<STRUCT<cityName: STRING, idCity: BIGINT, postalCode: STRING>>, 
					  countryName: STRING, departamentCode: STRING, departamentName: STRING, idDepartament: BIGINT>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8094/DSA-NoSQL-Neo4JService/rest/locations/DepartamentView')
        as data) json_raw
)
select v.idDepartament, v.departamentName, v.departamentCode, v.countryName, inline(v.cities) 
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM departaments_cities_view_all;





