----------------------------------------------------------------------------------
--- DSA_XLSX_SparkSQL_Views.sql
----------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8097/DSA-DOC-CSVService/rest/customers/CustomerEmployeesCategoryViewCSV');

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8097/DSA-DOC-CSVService/rest/customers/CustomerEmployeesCategoryViewCSV');

SELECT schema_of_json('[
	{"categoryCode":"E1","categoryName":"Small","lowerBound":1.0,"upperBound":99.0}
    ]');

-- 2. Create Remote View
-- DROP VIEW CTG_CUST_EMP_CSV_VIEW;
CREATE OR REPLACE VIEW CTG_CUST_EMP_CSV_VIEW AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<categoryCode: STRING, categoryName: STRING, lowerBound: DOUBLE, upperBound: DOUBLE>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8097/DSA-DOC-CSVService/rest/customers/CustomerEmployeesCategoryViewCSV')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM CTG_CUST_EMP_CSV_VIEW;