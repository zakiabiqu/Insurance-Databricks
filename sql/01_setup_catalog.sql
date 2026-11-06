CREATE CATALOG IF NOT EXISTS insurance_dev;
 
CREATE SCHEMA IF NOT EXISTS insurance_dev.landing;
CREATE SCHEMA IF NOT EXISTS insurance_dev.dm_analytics;
CREATE SCHEMA IF NOT EXISTS insurance_dev.ws_insurance;
CREATE SCHEMA IF NOT EXISTS insurance_dev.ops;
 
CREATE VOLUME IF NOT EXISTS insurance_dev.landing.raw_files;