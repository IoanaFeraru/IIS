----------------------------------------------------------------------------------
--- DS5_MogoDB_SparkSQL_Views.sql
----------------------------------------------------------------------------------
-- 1. Create JSON View
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'DEPARTAMENTS_JSON_VIEW',
               'http://localhost:8093/DSA-NoSQL-MongoDBService/rest/locations/DepartamentView');

SELECT * FROM DEPARTAMENTS_JSON_VIEW;
-- 2. Create Remote View
-- DROP VIEW departaments_view;
CREATE OR REPLACE VIEW departaments_view AS
select v.idDepartament, v.departamentName, v.departamentCode, v.countryName
FROM DEPARTAMENTS_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM departaments_view;

----------------------------------------------------------------------------------
-- 1. Create JSON View
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CITIES_JSON_VIEW',
               'http://localhost:8093/DSA-NoSQL-MongoDBService/rest/locations/CityView');

SELECT * FROM CITIES_JSON_VIEW;

-- 2. Create Remote View
-- DROP VIEW cities_view;
CREATE OR REPLACE VIEW cities_view AS
select v.*
FROM CITIES_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM cities_view;

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
-- 2. Create Remote View
-- DROP VIEW departaments_cities_view;
CREATE OR REPLACE VIEW departaments_cities_view AS
select v.idDepartament, explode(v.cities.idCity) as idCity
FROM DEPARTAMENTS_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM departaments_cities_view;

----------------------------------------------------------------------------------
-- DROP VIEW departaments_cities_view_all;
CREATE OR REPLACE VIEW departaments_cities_view_all AS
select v.idDepartament, v.departamentName, v.departamentCode, v.countryName, inline(v.cities)
FROM DEPARTAMENTS_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM departaments_cities_view_all;





