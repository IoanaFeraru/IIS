--------------------------------------------------------------------------------
--- DS0_CSV_SparkSQL_Views_from_REST.sql
--- Seller Profiles CSV (External Data Source)
--- DSA-port: 8097  Context: /DSA-DOC-CSVService/rest
--------------------------------------------------------------------------------
--- 1. seller_profiles
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://host.docker.internal:8097/DSA-DOC-CSVService/rest/csv/seller_profiles');

SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CSV_SELLER_PROFILES_JSON_VIEW',
               'http://host.docker.internal:8097/DSA-DOC-CSVService/rest/csv/seller_profiles');

CREATE OR REPLACE VIEW csv_seller_profiles_view AS
SELECT v.*
FROM CSV_SELLER_PROFILES_JSON_VIEW AS json_view
LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM csv_seller_profiles_view;

SELECT countryCode, COUNT(*) AS cnt FROM csv_seller_profiles_view GROUP BY countryCode ORDER BY cnt DESC;
SELECT isVerified, COUNT(*) AS cnt FROM csv_seller_profiles_view GROUP BY isVerified;

