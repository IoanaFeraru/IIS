--------------------------------------------------------------------------------
--- DS1_PostgreSQL_SparkSQL_Views_from_REST.sql
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView');

SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerDetailsView');

SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerAddressesView');
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
--- JDBC Data Source Access Model ----------------------------------------------
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CUSTOMERS_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView');

SELECT * FROM CUSTOMERS_JSON_VIEW;
---
-- DROP VIEW customers_view;
CREATE OR REPLACE VIEW customers_view AS
select v.*
FROM CUSTOMERS_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM customers_view;
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CUSTOMER_DETAILS_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerDetailsView');

select * FROM CUSTOMER_DETAILS_JSON_VIEW;
---
-- DROP VIEW customers_details_view;
CREATE OR REPLACE VIEW customers_details_view AS
select v.customerId, v.creditRating, v.age, v.turnover, v.nrOfEmps, v.compType
FROM CUSTOMER_DETAILS_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM customers_details_view;
--------------------------------------------------------------------------------
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CUSTOMER_ADDRESSES_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerAddressesView');

select * FROM CUSTOMER_ADDRESSES_JSON_VIEW;
---
-- DROP VIEW customers_addresses_view;
CREATE OR REPLACE VIEW customers_addresses_view AS
select v.*
-- v.customerId, v.address, v.city, v.countryCode, v.email, v.postalCode, v.socialPageUrl, v.socialProvider, v.websiteUrl
FROM CUSTOMER_ADDRESSES_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM customers_addresses_view;
--------------------------------------------------------------------------------

-- With AUTHENTICATION
SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CUSTOMER_ADDRESSES_JSON_VIEW',
               'http://developer:iis@localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerAddressesView');

select * FROM CUSTOMER_ADDRESSES_JSON_VIEW;
---
-- DROP VIEW customers_addresses_view;
CREATE OR REPLACE VIEW customers_addresses_view AS
select v.*
-- v.customerId, v.address, v.city, v.countryCode, v.email, v.postalCode, v.socialPageUrl, v.socialProvider, v.websiteUrl
FROM CUSTOMER_ADDRESSES_JSON_VIEW as json_view LATERAL VIEW explode(json_view.array) AS v;

SELECT * FROM customers_addresses_view;