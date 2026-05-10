--------------------------------------------------------------------------------
--- DS1_PostgreSQL_SparkSQL_Views.sql
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

-- 1. Get Data Source JSON Schema
-- 2. Create Remote View
-- 3. Test Remote View
--------------------------------------------------------------------------------
--- JDBC Data Source Access Model ----------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView');

SELECT schema_of_json('[{"customerId":1001,"name":"Alfa SRL","registrationcode":"R1001"}]');

-- 2. Create Remote View
-- DROP VIEW customers_view;
CREATE OR REPLACE VIEW customers_view AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<customerId: BIGINT, name: STRING, registrationcode: STRING>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView')
        as data) json_raw
)
select v.*
FROM json_view LATERAL VIEW explode(json_view.array) AS v;

-- 3. Test Remote View
SELECT * FROM customers_view;

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerDetailsView');

SELECT schema_of_json('[{"customerId":1001,"creditRating":"L","industry":"IT","age":3,"turnover":100000.0,"nrOfEmps":25.0,"compType":"SRL"}]');

-- 2. Create Remote View
-- DROP VIEW customers_details_view;
CREATE OR REPLACE VIEW customers_details_view AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<age: BIGINT, compType: STRING, creditRating: STRING, customerId: BIGINT, industry: STRING, nrOfEmps: DOUBLE, turnover: DOUBLE>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerDetailsView')
        as data) json_raw
)
select v.customerId, v.creditRating, v.age, v.turnover, v.nrOfEmps, v.compType
FROM json_view LATERAL VIEW explode(json_view.array) AS v;

-- 3. Test Remote View
SELECT * FROM customers_details_view;

----------------------------------------------------------------------------------
-- 1. Get Data Source JSON Schema
SELECT java_method(
               'org.spark.service.rest.QueryRESTDataService',
               'getRESTDataDocument',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerAddressesView');

SELECT schema_of_json('[{"customerId":1001,"address":"STR. Alfastreet Nr. 1","city":"IASI","countryCode":"RO","postalCode":"7001001"
	,"email":"alfa.srl@mail.com","socialProvider":"FB","socialPageUrl":"www.facebook.com/alfa.srl","websiteUrl":"www.alfa.soft.ro"}]');

-- 2. Create Remote View
-- DROP VIEW customers_addresses_view;
CREATE OR REPLACE VIEW customers_addresses_view AS
WITH json_view AS (
    SELECT from_json(json_raw.data,
                     'ARRAY<STRUCT<customerId: BIGINT, city: STRING, address: STRING, countryCode: STRING, email: STRING, 
					  postalCode: STRING, socialPageUrl: STRING, socialProvider: STRING, websiteUrl: STRING>>') array
    FROM (SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
        'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerAddressesView')
        as data) json_raw
)
select v.*
-- v.customerId, v.address, v.city, v.countryCode, v.email, v.postalCode, v.socialPageUrl, v.socialProvider, v.websiteUrl
FROM json_view LATERAL VIEW explode(json_view.array) AS v;

-- 3. Test Remote View
SELECT * FROM customers_addresses_view;


