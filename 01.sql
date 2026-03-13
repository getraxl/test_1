select $1 from  @MIGRATE_ADMIN.PUBLIC.DBT_PROJECT_FILES/dbt_project_files/models/bronze/raw_customer_address.sql

SELECT $1 FROM @MIGRATE_ADMIN.PUBLIC.DBT_PROJECT_FILES/dbt_project_files/models/bronze/raw_customer_address.sql (FILE_FORMAT => (TYPE = 'CSV' FIELD_DELIMITER = NONE RECORD_DELIMITER = NONE))


