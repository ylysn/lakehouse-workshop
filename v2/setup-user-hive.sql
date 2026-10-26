-- Lakehouse HOL v2 (CDP 7.3.2): run in the Hue *Hive* editor, after the Impala user setup below
-- The planes table is created by Hive so that Spark can migrate it in place (Lab 2).
-- (A table whose files Impala wrote fails to read after Spark's migrate: "UUID require 16 bytes".)
drop table if exists ${user_id}_airlines.planes;

CREATE TABLE ${user_id}_airlines.planes (
  tailnum STRING, owner_type STRING, manufacturer STRING, issue_date STRING,
  model STRING, status STRING, aircraft_type STRING,  engine_type STRING, year INT
)
STORED AS PARQUET
TBLPROPERTIES ( 'transactional'='false' );

INSERT INTO ${user_id}_airlines.planes
  SELECT * FROM airlines_csv.planes_csv;
