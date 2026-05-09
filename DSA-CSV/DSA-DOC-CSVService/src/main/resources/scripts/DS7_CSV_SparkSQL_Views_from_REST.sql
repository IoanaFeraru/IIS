----------------------------------------------------------------------------------
--- DSA_XLSX_SparkSQL_Views.sql
----------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8097/DSA-DOC-CSVService/rest/customers/CustomerEmployeesCategoryViewCSV');

----------------------------------------------------------------------------------
-- 1. Create JSON View
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CTG_CUST_EMP_CSV_JSON_VIEW',
               'http://localhost:8097/DSA-DOC-CSVService/rest/customers/CustomerEmployeesCategoryViewCSV');

SELECT * FROM CTG_CUST_EMP_CSV_JSON_VIEW;
-- 2. Create Remote View
-- DROP VIEW CTG_CUST_EMP_CSV_VIEW;
CREATE OR REPLACE VIEW CTG_CUST_EMP_CSV_VIEW AS
select v.*
FROM CTG_CUST_EMP_CSV_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;
-- 3. Test Remote View
select * FROM CTG_CUST_EMP_CSV_VIEW;