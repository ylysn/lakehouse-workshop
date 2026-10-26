# CDP 7.3.2 Lakehouse - Hands-on Labs v2.0
_Iceberg multi-function analytics on Cloudera on premises 7.3.2: airline use case_


## Contents

- [Overview](#overview)
- [Hands-on Lab Setup](#hands-on-lab-setup)
    - [One cluster, many lab users](#one-cluster-many-lab-users)
    - [Cluster provisioning](#cluster-provisioning)
    - [Getting access](#getting-access)
    - [Lab administrator: load the airline data](#lab-administrator-load-the-airline-data)
    - [Lab administrator: pause and resume the cluster](#lab-administrator-pause-and-resume-the-cluster)
    - [Every lab user: create your databases](#every-lab-user-create-your-databases)
- [Lab 1 - Iceberg with NiFi and SQL Stream Builder](#lab-1---iceberg-with-nifi-and-sql-stream-builder)
    - [NiFi: create a keytab for the flow](#nifi-create-a-keytab-for-the-flow)
    - [NiFi: import the flow](#nifi-import-the-flow)
    - [NiFi: set the parameters](#nifi-set-the-parameters)
    - [NiFi: enable the services and start the flows](#nifi-enable-the-services-and-start-the-flows)
    - [Inspect the NiFi Iceberg tables](#inspect-the-nifi-iceberg-tables)
    - [SQL Stream Builder: import the project](#sql-stream-builder-import-the-project)
    - [SQL Stream Builder: unlock your keytab and set your environment](#sql-stream-builder-unlock-your-keytab-and-set-your-environment)
    - [SQL Stream Builder: data sources and virtual tables](#sql-stream-builder-data-sources-and-virtual-tables)
    - [SQL Stream Builder: run the jobs](#sql-stream-builder-run-the-jobs)
    - [Inspect the SSB Iceberg tables](#inspect-the-ssb-iceberg-tables)
- [Lab 2 - Spark (Multi-function Analytics)](#lab-2---spark-multi-function-analytics)
    - [Migrate existing tables to Iceberg](#migrate-existing-tables-to-iceberg)
    - [Query the Iceberg tables](#query-the-iceberg-tables)
    - [Create and load an Iceberg table](#create-and-load-an-iceberg-table)
    - [ACID: MERGE](#acid-merge)
    - [Table maintenance](#table-maintenance)
    - [Hard deletes](#hard-deletes)
- [Lab 3 - Data Warehouse with Impala](#lab-3---data-warehouse-with-impala)
    - [Migrate tables to Iceberg](#migrate-tables-to-iceberg)
    - [Create and load a partitioned Iceberg table](#create-and-load-a-partitioned-iceberg-table)
    - [Partition evolution](#partition-evolution)
    - [Time travel](#time-travel)
    - [Hive and Iceberg tables in one query](#hive-and-iceberg-tables-in-one-query)
- [Lab 4 - SDX: Fine-grained Access Control and Lineage](#lab-4---sdx-fine-grained-access-control-and-lineage)
    - [Ranger: mask a column](#ranger-mask-a-column)
    - [Atlas: metadata and lineage](#atlas-metadata-and-lineage)
- [Lab 5 - Replication](#lab-5---replication)
- [Lab 6 - Cloudera Lakehouse Optimizer](#lab-6---cloudera-lakehouse-optimizer)
    - [Look at the problem](#look-at-the-problem)
    - [Open the Lakehouse Optimizer](#open-the-lakehouse-optimizer)
    - [Create the policy](#create-the-policy)
    - [Dry run and run the policy](#dry-run-and-run-the-policy)
    - [Monitor the task](#monitor-the-task)
- [Cleanup](#cleanup)
- [Appendix](#appendix)
    - [Lab cluster settings](#lab-cluster-settings)
    - [Performance tuning with Iceberg metadata tables](#performance-tuning-with-iceberg-metadata-tables)
    - [Lab setup SQL](#lab-setup-sql)
    - [Workshop repository](#workshop-repository)

## Overview

This workshop shows the Open Data Lakehouse capabilities of Cloudera on premises 7.3.2 with Apache Iceberg as the common table format. You ingest data with **NiFi** and **SQL Stream Builder (Flink)**, process it with **Spark 3**, query it with **Impala** in **Hue**, and govern it with **Ranger** and **Atlas**, all on the same Iceberg tables.

Version 2.0 is a refresh of v1.1 (CDP 7.1.9) for 7.3.2. Every step and screenshot in this document was run and captured on a 7.3.2 Lakehouse cluster deployed with the `lakehouse-cluster.yml` playbook. The steps use the user id **admin**: replace it with your own user id.

**AT A GLANCE**

|  |  |
|---|---|
| **Platform** | Cloudera Manager 7.13.2<br>Cloudera Runtime 7.3.2 SP2 (7.3.2.20000)<br>RHEL 9.6 |
| **Services** | HDFS, Ozone, Hive, Impala, Hue, Spark 3.5, Kafka (KRaft), Ranger, Atlas, Knox, Solr, HBase, YARN<br>CSA 1.18: Flink 1.20.5 and SQL Stream Builder<br>CFM 4.13: NiFi 2.11 and NiFi Registry |
| **Labs** | 1. NiFi and SQL Stream Builder to Iceberg<br>2. Spark (multi-function analytics, table maintenance)<br>3. Impala / Hue (data warehouse, time travel, evolution)<br>4. SDX (Ranger masking, Atlas lineage)<br>5. Replication (No Labs)<br>6. Lakehouse Optimizer |
| **Time** | About 2.5 hours for the labs; 30 minutes of lab setup by the lab administrator.<br>Longest jobs: the Lab 3 load up to 2006 (about 6 minutes) and the Lab 6 Optimizer task (about 2 minutes). Every other step returns in under a minute, most in a few seconds. |
| **Access** | All UIs through the Knox gateway with single sign-on: `https://knox.<gateway-ip>.pvc.cloudera-labs.com/gateway/homepage/home` (the gateway IP as is, e.g. knox.13.57.98.89) |

**WHAT CHANGED FROM V1.1 (CDP 7.1.9)**

| Area | v2.0 on CDP 7.3.2 |
|---|---|
| **NiFi** | NiFi 2.11 (CFM 4.13). The flow is SSBDemo-v2.json: generic parameters, and the Kafka processors already use the Default Truststore-only SSL Context Service.<br>The parameter context is `lakehouse_hol_params`. |
| **Keytab for NiFi** | `ktutil` needs `-f` (salt from the KDC) with FreeIPA, or the keytab fails with "Preauthentication failed". |
| **SQL Stream Builder** | Unlock the keytab with your user id and password (no keytab upload needed). The **Local Kafka** data source is already configured for the cluster, so there is no Kafka data source form to fill in. Stop each job when you finish with it. |
| **Lab setup** | The `planes` table is created in the Hue **Hive** editor. The flights data is a split tar archive. `hol-setup.sh` loads everything. |
| **Spark** | Spark 3.5.8 on Python 3.11 (`pyspark3`). `migrate` keeps a `<table>_backup_` copy. |
| **Impala** | In-place migration is now available: `ALTER TABLE … CONVERT TO ICEBERG`. Hive also supports Iceberg in 7.3.2. |
| **Replication (Lab 5)** | Not covered in v2.0: it needs a second cluster. |
| **Lakehouse Optimizer (Lab 6)** | New lab: a policy compacts the small files and expires the old snapshots that the Lab 1 streaming jobs leave behind. |

> **Warning:** The lab cluster is a single host. SQL Stream Builder runs every job in one shared Flink session with a limited number of slots: **stop each SSB job before you start the next one**, or new jobs wait for a slot and show no results.


## Hands-on Lab Setup


### One cluster, many lab users

One lab cluster serves all participants. Each participant works as their own user, for example `user01`, `user02`, `user03`, and gets their own databases (`user01_airlines`, `user01_airlines_maint`), NiFi process group, SSB environment and Ranger policy, so users do not affect each other's work.

- **This guide uses the user id `admin` in every example and screenshot.** Wherever you see `admin` or `<user_id>`, use your own user id; in Hue, enter it for the `${user_id}` parameter.
- The lab administrator runs the setup once for everyone and passes the list of lab users to it (for example `admin user01 user02 user03`), so each of them gets an HDFS home directory.
- The lab users must exist in the cluster's FreeIPA directory (they log in to Knox and Hue with it). The lab administrator creates them in FreeIPA before the lab.

> **Note:** The SSB project can be imported only once per cluster under the name SSB-Iceberg-Demo: the first user imports it and invites the others (Members), or each user forks the repository and imports it under their own project name.


### Cluster provisioning

The cluster is provisioned using Cloudera Community Edition – One Node Cluster on AWS, with its `lakehouse-cluster.yml` playbook for the Cloudera services these labs need. Ask your lab administrator or the GOES team for access.


### Getting access

Open the Knox homepage and log in with your user id and password (the lab password is the same for every service). All lab UIs open from here; you do not need SSH for Labs 1, 3 and 4.

![screenshots/00-knox-homepage.png](screenshots/00-knox-homepage.png)

> **Note:** Lab 2 (Spark) uses `pyspark3` on the cluster host: `ssh <user>@<gateway>` as provided by the lab administrator, then `kinit` with your password.


### Lab administrator: load the airline data

The lab administrator does this once per cluster, on the cluster base host. It takes about 5 minutes.

**Step 1.** SSH to the base host through the gateway, with the SSH key created by the deployment (`<prefix>-ssh-key.pem`):

```bash
ssh -i <prefix>-ssh-key.pem -J ec2-user@<gateway-ip> ec2-user@<prefix>-master-01.internal.cloudera-labs.com

# example
ssh -i jsmith-ce-ssh-key.pem -J ec2-user@13.57.98.89 ec2-user@jsmith-ce-master-01.internal.cloudera-labs.com
```

> **Note:** The base host name only resolves inside the cluster network; `-J` connects through the gateway, which resolves it. The first time, answer `yes` to confirm the host keys of the gateway and the base host.

**Step 2.** On the base host (in the SSH session from Step 1), clone the workshop repository and run the setup script with the lab password and the lab user ids:

```bash
sudo dnf -y install git      # already installed by lakehouse-cluster.yml
git clone https://github.com/ylysn/lakehouse-workshop.git
cd lakehouse-workshop/v2
./hol-setup.sh '<admin password>' admin user01 user02 user03
```

The script downloads the airline data from [airlines-source-data](https://github.com/jingalls1217/airlines-source-data) (the flights file is a split tar archive), loads it into HDFS under `/data/airlines-csv`, creates an HDFS home directory for every lab user, and creates the `airlines_csv` tables in Hive (the SQL is in the appendix, Lab setup SQL). It ends with the size of each data set and beeline finishing without errors:

```bash
35.5 K   35.5 K   /data/airlines-csv/airlines
200.0 K  200.0 K  /data/airlines-csv/airports
7.4 G    7.4 G    /data/airlines-csv/flights
422.5 K  422.5 K  /data/airlines-csv/planes
7.5 M    7.5 M    /data/airlines-csv/unique_tickets
```

**Step 3.** Check the data in Hue: open the Knox homepage and click **Hue UI**.

![screenshots/setup-02-knox-open-hue.png](screenshots/setup-02-knox-open-hue.png)

In the **Impala** editor, run the count below. It returns 86,289,323 rows.

```bash
SELECT COUNT(*) FROM airlines_csv.flights_csv;
```

![screenshots/setup-03-count-flights.png](screenshots/setup-03-count-flights.png)


### Lab administrator: pause and resume the cluster

For a workshop that spans several days, stop the cluster between sessions instead of rebuilding it. From the deployment project folder:

```bash
ansible-navigator run pause.yml -e @config.yml     # stop the instances
ansible-navigator run resume.yml -e @config.yml    # start the instances
```

The gateway IP, the Knox URL, the users and every lab table stay the same, so participants continue where they stopped. Resume well before the session starts.

> **Warning:** Cloudera Manager does not bring the cluster back after a non-graceful shutdown. Before `pause.yml`, stop the cluster in Cloudera Manager (cluster **Actions** → **Stop**). After `resume.yml`, start it (cluster **Actions** → **Start**) and wait until every service is green.


### Every lab user: create your databases

1. Open **Hue** from the Knox homepage. The editor opens on **Impala**.
2. Copy the SQL below into the editor. Hue detects the `${user_id}` parameter: enter your user id in the box. Select all and run it. It runs 44 statements in a few minutes: the three loads into `${user_id}_airlines_maint.flights` take about 10 seconds each, the other statements 2-3 seconds each.

```bash
-- Lakehouse HOL v2 (CDP 7.3.2): run in the Hue *Impala* editor (enter your user id for ${user_id})
-- CREATE DATABASES
-- EACH USER RUNS TO CREATE DATABASES
CREATE DATABASE ${user_id}_airlines;
CREATE DATABASE ${user_id}_airlines_maint;


-- HIVE TABLE FORMAT TO USE CTAS TO CONVERT TO ICEBERG
drop table if exists ${user_id}_airlines.airports_hive;

CREATE TABLE ${user_id}_airlines.airports_hive
   AS SELECT * FROM airlines_csv.airports_csv;

-- HIVE TABLE FORMAT
drop table if exists ${user_id}_airlines.unique_tickets;

CREATE TABLE ${user_id}_airlines.unique_tickets (
  ticketnumber BIGINT, leg1flightnum BIGINT, leg1uniquecarrier STRING,
  leg1origin STRING,   leg1dest STRING, leg1month BIGINT,
  leg1dayofmonth BIGINT, leg1dayofweek BIGINT, leg1deptime BIGINT,
  leg1arrtime BIGINT, leg2flightnum BIGINT, leg2uniquecarrier STRING,
  leg2origin STRING, leg2dest STRING, leg2month BIGINT, leg2dayofmonth BIGINT,
  leg2dayofweek BIGINT, leg2deptime BIGINT, leg2arrtime BIGINT 
);

INSERT INTO ${user_id}_airlines.unique_tickets
  SELECT * FROM airlines_csv.unique_tickets_csv;

-- CREATE ICEBERG TABLE FORMAT STORED AS PARQUET
drop table if exists ${user_id}_airlines.planes_iceberg;

CREATE TABLE ${user_id}_airlines.planes_iceberg
   STORED AS ICEBERG AS
   SELECT * FROM airlines_csv.planes_csv;

-- CREATE ICEBERG TABLE FORMAT STORED AS PARQUET
drop table if exists ${user_id}_airlines.flights_iceberg;

CREATE TABLE ${user_id}_airlines.flights_iceberg (
 month int, dayofmonth int, 
 dayofweek int, deptime int, crsdeptime int, arrtime int, 
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string, 
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, 
 depdelay int, origin string, dest string, distance int, taxiin int, 
 taxiout int, cancelled int, cancellationcode string, diverted string, 
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, 
 lateaircraftdelay int
) 
PARTITIONED BY (year int)
STORED AS ICEBERG 
;

-- LOAD DATA INTO ICEBERG TABLE FORMAT STORED AS PARQUET
INSERT INTO ${user_id}_airlines.flights_iceberg
 SELECT * FROM airlines_csv.flights_csv
 WHERE year <= 2006;

--
-- TABLES NEEDED FOR THE NIFI LAB
DROP TABLE IF EXISTS ${user_id}_airlines.`routes_nifi_iceberg`;
CREATE TABLE ${user_id}_airlines.`routes_nifi_iceberg` (
  `airline_iata` VARCHAR,
  `airline_icao` VARCHAR,
  `departure_airport_iata` VARCHAR,
  `departure_airport_icao` VARCHAR,
  `arrival_airport_iata` VARCHAR,
  `arrival_airport_icao` VARCHAR,
  `codeshare` BOOLEAN,
  `transfers` BIGINT,
  `planes` ARRAY<VARCHAR>
) STORED AS ICEBERG;

DROP TABLE IF EXISTS ${user_id}_airlines.`airports_nifi_iceberg`;
CREATE TABLE ${user_id}_airlines.`airports_nifi_iceberg` (
  `city_code` VARCHAR,
  `country_code` VARCHAR,
  `name_translations` STRUCT<`en`:string>,
  `time_zone` VARCHAR,
  `flightable` BOOLEAN,
  `coordinates` struct<`lat`:DOUBLE, `lon`:DOUBLE>,
  `name` VARCHAR,
  `code` VARCHAR,
  `iata_type` VARCHAR
) STORED AS ICEBERG;

DROP TABLE IF EXISTS ${user_id}_airlines.`countries_nifi_iceberg`;
CREATE TABLE ${user_id}_airlines.`countries_nifi_iceberg` (
  `name_translations` STRUCT<`en`:VARCHAR>,
  `cases` STRUCT<`su`:VARCHAR>,
  `code` VARCHAR,
  `name` VARCHAR,
  `currency` VARCHAR
) STORED AS ICEBERG;

-- [TABLE MAINTENANCE] CREATE FLIGHTS TABLE IN ICEBERG TABLE FORMAT
drop table if exists ${user_id}_airlines_maint.flights;

CREATE TABLE ${user_id}_airlines_maint.flights (
 month int, dayofmonth int, 
 dayofweek int, deptime int, crsdeptime int, arrtime int, 
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string, 
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, 
 depdelay int, origin string, dest string, distance int, taxiin int, 
 taxiout int, cancelled int, cancellationcode string, diverted string, 
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, 
 lateaircraftdelay int
) 
PARTITIONED BY (year int)
STORED AS ICEBERG 
;

-- LOAD DATA TO SIMULATE SMALL FILES
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 1;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 2;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 3;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 4;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 5;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 6;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 7;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 8;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 9;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 10;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 11;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1995 AND month = 12;

INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 1;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 2;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 3;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 4;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 5;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 6;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 7;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 8;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 9;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 10;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 11;
INSERT INTO ${user_id}_airlines_maint.flights
 SELECT * FROM airlines_csv.flights_csv WHERE year = 1996 AND month = 12;
```

![screenshots/setup-04-user-tables-impala.png](screenshots/setup-04-user-tables-impala.png)

1. Switch the editor to **Hive** (the editor drop-down next to the editor name) and run the `planes` table setup below, again with your user id. Lab 2 migrates this table in place with Spark, which needs a table that Hive wrote. The insert takes about 20 seconds.

```bash
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
```

![screenshots/setup-08-planes-in-hive.png](screenshots/setup-08-planes-in-hive.png)

Back in the **Impala** editor, check the tables:

```bash
SELECT COUNT(*) FROM airlines_csv.planes_csv;

DESCRIBE FORMATTED ${user_id}_airlines.planes;

SELECT * FROM ${user_id}_airlines.planes LIMIT 10;
```

![screenshots/setup-05-count-planes.png](screenshots/setup-05-count-planes.png)

Look at **Table Type**, **SerDe Library** and **Location**: `planes` is a Hive-format table (Parquet).

![screenshots/setup-06-describe-planes.png](screenshots/setup-06-describe-planes.png)

The tail number is filled in; most other columns are empty in the source data.

![screenshots/setup-07-select-planes.png](screenshots/setup-07-select-planes.png)


## Lab 1 - Iceberg with NiFi and SQL Stream Builder

NiFi ingests three public JSON data sets (airline routes, countries, airports), publishes each record to Kafka, and writes it directly to an Iceberg table. SQL Stream Builder (SSB) then reads the Kafka topics with SQL and writes them to Iceberg tables as a streaming job. Finally you query all of these tables in Hue.

> **Note:** The NiFi flow and the SSB project were created by **Steven Matison** (Cloudera) for the CDP 7.1.9 lab ([NiFi-Templates](https://github.com/cldr-steven-matison/NiFi-Templates), [SSB-Iceberg-Demo](https://github.com/cldr-steven-matison/SSB-Iceberg-Demo)). v2 adapts them for 7.3.2; the workshop repository keeps a copy.


### NiFi: create a keytab for the flow

The NiFi flow writes to Iceberg as your user, with a keytab. SSH to the cluster host and create it:

```bash
ktutil
addent -password -p <user_id> -k 1 -e aes256-cts -f
(enter your password)
wkt /tmp/<user_id>.keytab
quit

kinit -kt /tmp/<user_id>.keytab <user_id>    # must succeed
sudo chown nifi:nifi /tmp/<user_id>.keytab
```

> **Note:** The `-f` option takes the salt from the KDC. FreeIPA does not use the default salt, so without `-f` the keytab is written but every login with it fails with "Preauthentication failed".


### NiFi: import the flow

Download the flow definition **SSBDemo-v2.json** from the workshop repository ([https://github.com/ylysn/lakehouse-workshop/blob/main/v2/SSBDemo-v2.json](https://github.com/ylysn/lakehouse-workshop/blob/main/v2/SSBDemo-v2.json), **Download raw file**). Then open **Nifi UI** from the Knox homepage.

1. Drag a **Process Group** from the toolbar onto the canvas.
    ![screenshots/lab1-nifi-01-add-process-group.png](screenshots/lab1-nifi-01-add-process-group.png)

2. In **Create Process Group**, click the upload icon at the right end of the Name field and select **SSBDemo-v2.json**.
    ![screenshots/lab1-nifi-02-create-process-group.png](screenshots/lab1-nifi-02-create-process-group.png)

3. After the file is attached, set the name to `<user_id>_SSBDemo` (attaching the file replaces the name with the file name) and click **Add**.
    ![screenshots/lab1-nifi-03-flow-file-selected.png](screenshots/lab1-nifi-03-flow-file-selected.png)

4. The process group appears on the canvas. It stays invalid until the parameters are set.
    ![screenshots/lab1-nifi-04-process-group-imported.png](screenshots/lab1-nifi-04-process-group-imported.png)



### NiFi: set the parameters

1. Open the menu (☰, top right) → **Parameter Contexts**.
    ![screenshots/lab1-nifi-05-menu-parameter-contexts.png](screenshots/lab1-nifi-05-menu-parameter-contexts.png)

2. On `lakehouse_hol_params`, click ⋮ → **Edit**, open the **Parameters** tab and set each value (⋮ → Edit on the parameter):
    ![screenshots/lab1-nifi-06-parameters-placeholders.png](screenshots/lab1-nifi-06-parameters-placeholders.png)


| Parameter | Value |
|---|---|
| CDP Workload User | your user id, e.g. `admin` |
| CDP Workload User Password | your password (sensitive) |
| Hadoop Configuration Resources | `/etc/hive/conf/hive-site.xml,/etc/hadoop/conf/core-site.xml,/etc/hive/conf/hdfs-site.xml` (keep) |
| Hive Metastore URI | `thrift://<base-host>:9083` |
| Kafka Broker Endpoint | `<base-host>:9093` |
| Kerberos Keytab | `/tmp/<user_id>.keytab` |

`<base-host>` is the cluster base host, for example `<prefix>-master-01.internal.cloudera-labs.com` (`jsmith-ce-master-01.internal.cloudera-labs.com` for the prefix `jsmith-ce`). Click **Apply**.

![screenshots/lab1-nifi-07-parameters-set.png](screenshots/lab1-nifi-07-parameters-set.png)

> **Note:** If several users share the cluster, each user imports the flow into their own process group with their own parameter context (create a new one from the process group's **Parameters** menu) so the values do not collide.


### NiFi: enable the services and start the flows

1. Double-click the `<user_id>_SSBDemo` process group. Right-click an empty part of the canvas → **Enable All Controller Services**.
    ![screenshots/lab1-nifi-08-enable-controller-services.png](screenshots/lab1-nifi-08-enable-controller-services.png)

2. Right-click the canvas again → **Start**. (v1.1 asked you to set the SSL Context Service on each PublishKafka processor: the v2 flow already has it.)
    ![screenshots/lab1-nifi-09-start-flow.png](screenshots/lab1-nifi-09-start-flow.png)

3. Data flows within a minute. Open each process group to watch the queues; nothing should go to the failure relationships.
    ![screenshots/lab1-nifi-10-flow-running.png](screenshots/lab1-nifi-10-flow-running.png)

    ![screenshots/lab1-nifi-12-routes-flow.png](screenshots/lab1-nifi-12-routes-flow.png)

    ![screenshots/lab1-nifi-13-airports-flow.png](screenshots/lab1-nifi-13-airports-flow.png)

    ![screenshots/lab1-nifi-14-countries-flow.png](screenshots/lab1-nifi-14-countries-flow.png)


> **Note:** PutIceberg commits in small batches: the Iceberg tables fill over a few minutes, while Kafka receives every record almost at once. The warning on "end of flow" is only the unconnected output port.


### Inspect the NiFi Iceberg tables

In the Hue **Impala** editor:

```bash
SELECT * FROM ${user_id}_airlines.`routes_nifi_iceberg`;
```

![screenshots/lab1-nifi-20-routes-iceberg.png](screenshots/lab1-nifi-20-routes-iceberg.png)

```bash
SELECT * FROM ${user_id}_airlines.`airports_nifi_iceberg`;
```

![screenshots/lab1-nifi-21-airports-iceberg.png](screenshots/lab1-nifi-21-airports-iceberg.png)

```bash
select * from ${user_id}_airlines.countries_nifi_iceberg;
```

![screenshots/lab1-nifi-22-countries-iceberg.png](screenshots/lab1-nifi-22-countries-iceberg.png)


### SQL Stream Builder: import the project

Open **SSB** (SQL Stream Builder) from the Knox homepage. Do not create a new project: import it from GitHub in one step.

1. Click **Import**. Clone URL `https://github.com/ylysn/lakehouse-workshop`, branch `main`, project `SSB-Iceberg-Demo`, authentication off. Click **Import**.
    ![screenshots/lab1-ssb-01-import-project.png](screenshots/lab1-ssb-01-import-project.png)

2. The summary lists the data source (Hive catalog), 3 virtual tables, 6 jobs and an API key.
    ![screenshots/lab1-ssb-02-import-summary.png](screenshots/lab1-ssb-02-import-summary.png)

3. Click **Switch** on SSB-Iceberg-Demo and confirm.
    ![screenshots/lab1-ssb-03-switch-project.png](screenshots/lab1-ssb-03-switch-project.png)


> **Note:** Only one user can import a project with this name per cluster. Other users either get invited to the project (Members), or fork the repository and import it under their own project name.


### SQL Stream Builder: unlock your keytab and set your environment

1. The project page asks you to unlock your keytab. Click **unlock your keytab**.
    ![screenshots/lab1-ssb-04-project-overview.png](screenshots/lab1-ssb-04-project-overview.png)

2. Enter your user id as Principal Name and your password, then click **Unlock Keytab**. (Uploading a keytab also works, but is not needed.)
    ![screenshots/lab1-ssb-05-keytab-unlock.png](screenshots/lab1-ssb-05-keytab-unlock.png)

    ![screenshots/lab1-ssb-06-keytab-ok.png](screenshots/lab1-ssb-06-keytab-ok.png)

3. Open **Environments** (left bar) → **New Environment**. Name it `<user_id>_env`, add the key `userid` with your user id as value, click **Add**, then **Create**.
    ![screenshots/lab1-ssb-07-environments.png](screenshots/lab1-ssb-07-environments.png)

    The jobs refer to your database as `${ssb.env.userid}_airlines`: SSB replaces `${ssb.env.userid}` with the value of the `userid` key in your active environment. For example, user `user01` creates the environment `user01_env` with key `userid` and value `user01`, so the jobs write to `user01_airlines`. The screenshot uses `admin`, which writes to `admin_airlines`.

    ![screenshots/lab1-ssb-08-new-environment.png](screenshots/lab1-ssb-08-new-environment.png)

4. Click **Activate** and confirm. The environment name shows next to the project name.
    ![screenshots/lab1-ssb-09-activate-environment.png](screenshots/lab1-ssb-09-activate-environment.png)

    ![screenshots/lab1-ssb-10-environment-active.png](screenshots/lab1-ssb-10-environment-active.png)



### SQL Stream Builder: data sources and virtual tables

The **Local Kafka** data source points at the cluster's Kafka (SASL_SSL with Kerberos). It is already there: you do not need to create a Kafka data source.

![screenshots/lab1-ssb-11-local-kafka.png](screenshots/lab1-ssb-11-local-kafka.png)

1. In the Explorer, open **Virtual Tables** → `airports_kafka`. Select the topic `<user_id>_airports` (written by your NiFi flow) under **Topic Name**.
    ![screenshots/lab1-ssb-12-topic-name.png](screenshots/lab1-ssb-12-topic-name.png)

2. SSB detects the schema from the topic. Click **Save**.
    ![screenshots/lab1-ssb-13-detect-schema.png](screenshots/lab1-ssb-13-detect-schema.png)

3. Repeat for `countries_kafka` (`<user_id>_countries`) and `routes_kafka` (`<user_id>_routes`).

> **Note:** If Detect Schema reports "Record value is empty", the topic holds an empty message: close the message and keep the imported schema, which already matches the data. Wait for the detection to finish before you click Save.


### SQL Stream Builder: run the jobs

Run the statements of each job one at a time: select a statement and click **Execute Selection**. Do not run a whole job at once.

1. Open the job **Airports_Kafka**. Select `SELECT * FROM airports_kafka;` and click **Execute Selection**. The first job of a session can take 1-2 minutes to show results.
    ![screenshots/lab1-ssb-14-execute-selection.png](screenshots/lab1-ssb-14-execute-selection.png)

    ![screenshots/lab1-ssb-15-kafka-results.png](screenshots/lab1-ssb-15-kafka-results.png)

2. **Stop** the job. Select the `CREATE TABLE airports_kafka_iceberg` statement and execute it: it creates the Iceberg connector (a virtual table in SSB; the Impala table is created by the next step).
3. Select the `INSERT INTO airports_kafka_iceberg SELECT * FROM airports_kafka;` statement and execute it. This is a streaming job: it keeps writing Kafka records to the Iceberg table, committing every 30 seconds.
    ![screenshots/lab1-ssb-16-insert-into-iceberg.png](screenshots/lab1-ssb-16-insert-into-iceberg.png)

4. After a minute, run the last select (from the Iceberg table) or query the table in Hue. Then **Stop** the job.
5. Do the same with **Countries_Kafka** and **Routes_Kafka**, stopping each job when you are done.
6. **CSA_1_11_Iceberg_Sample** creates a table in `default`, inserts one row and reads it back. **Test_Hue_Tables** reads the `flights_iceberg` table created by Impala in the setup.
    ![screenshots/lab1-ssb-17-test-hue-tables.png](screenshots/lab1-ssb-17-test-hue-tables.png)


> **Warning:** Stop every SSB job when you finish with it. Running jobs keep their Flink slots; on the single-host lab cluster new jobs then wait for a slot and show "No Results".


### Inspect the SSB Iceberg tables

```bash
select * from ${user_id}_airlines.airports_kafka_iceberg;
```

![screenshots/lab1-ssb-20-airports-kafka-iceberg.png](screenshots/lab1-ssb-20-airports-kafka-iceberg.png)

```bash
select * from ${user_id}_airlines.countries_kafka_iceberg;
```

![screenshots/lab1-ssb-21-countries-kafka-iceberg.png](screenshots/lab1-ssb-21-countries-kafka-iceberg.png)


## Lab 2 - Spark (Multi-function Analytics)

Iceberg is engine agnostic: the tables you created with Impala are now processed with Spark 3. Connect to the cluster host. Set your user id once in the shell (the commands below and every Spark statement use it), get a Kerberos ticket and start `pyspark3` (Spark 3.5.8, Python 3.11):

```bash
export USERNAME=admin        # your user id
kinit $USERNAME
pyspark3
```

![screenshots/lab2-00-pyspark3-shell.png](screenshots/lab2-00-pyspark3-shell.png)

Every Spark step in this lab returns within seconds; all of them together take about 3 minutes.

> **Note:** Spark needs an HDFS home directory for your user (`/user/<user_id>`). `hol-setup.sh` creates it for the users you pass. Without it Spark fails with "Permission denied: user=<user_id>, access=WRITE, inode=\"/user\"".

At the `>>>` prompt, read your user id from the shell, so every statement below can be pasted as is:

```bash
import os
USERNAME = os.environ["USERNAME"]
```


### Migrate existing tables to Iceberg

**Create a copy in Iceberg format (CTAS).** This works for any Hive table, including ACID (transactional) tables:

```bash
spark.sql(f"CREATE EXTERNAL TABLE {USERNAME}_airlines.planes_de USING ICEBERG TBLPROPERTIES('format-version'='2') AS SELECT * FROM {USERNAME}_airlines.planes").show()
```

![screenshots/lab2-01-ctas-planes-de.png](screenshots/lab2-01-ctas-planes-de.png)

You would normally drop the original Hive table now (do not run it; next you migrate the same table in place):

```bash
spark.sql(f"DROP TABLE {USERNAME}_airlines.planes").show()
```

**Migrate in place.** The data files stay where they are; only Iceberg metadata is written. This works for non-transactional tables. First check the format of `planes`:

```bash
spark.sql(f"DESCRIBE FORMATTED {USERNAME}_airlines.planes").show(100)
```

![screenshots/lab2-02-planes-hive-format.png](screenshots/lab2-02-planes-hive-format.png)

```bash
spark.sql(f"CALL spark_catalog.system.migrate('{USERNAME}_airlines.planes', map('format-version','2'))").show()
```

![screenshots/lab2-03-migrate-in-place.png](screenshots/lab2-03-migrate-in-place.png)

```bash
spark.sql(f"DESCRIBE FORMATTED {USERNAME}_airlines.planes").show(100, False)
```

![screenshots/lab2-04-planes-iceberg-format.png](screenshots/lab2-04-planes-iceberg-format.png)

> **Note:** `migrate` keeps the original table as `<table>_backup_`. Drop it before you migrate a table with the same name again.


### Query the Iceberg tables

```bash
spark.sql(f"SELECT * FROM {USERNAME}_airlines.planes LIMIT 20").show()
```

![screenshots/lab2-04b-query-planes.png](screenshots/lab2-04b-query-planes.png)

The first rows have mostly empty columns. Qualify the query:

```bash
spark.sql(f"SELECT * FROM {USERNAME}_airlines.planes_de WHERE year > 2000 LIMIT 20").show()
```

![screenshots/lab2-05-query-planes-de.png](screenshots/lab2-05-query-planes-de.png)


### Create and load an Iceberg table

```bash
spark.sql(f"CREATE EXTERNAL TABLE {USERNAME}_airlines.airlines_de (code string, description string) USING ICEBERG TBLPROPERTIES ('format-version' = '2')").show()
spark.sql(f"SELECT * FROM {USERNAME}_airlines.airlines_de limit 20").show()
```

![screenshots/lab2-07-airlines-de-empty.png](screenshots/lab2-07-airlines-de-empty.png)

```bash
spark.sql(f"INSERT INTO {USERNAME}_airlines.airlines_de SELECT * FROM airlines_csv.airlines_csv").show()
spark.sql(f"SELECT * FROM {USERNAME}_airlines.airlines_de WHERE code ='UA'").show()
```

![screenshots/lab2-09-ua-before.png](screenshots/lab2-09-ua-before.png)


### ACID: MERGE

Rename United Airlines to "Adrenaline Airways" with an ACID MERGE:

```bash
spark.sql(f'MERGE INTO {USERNAME}_airlines.airlines_de s USING (SELECT t.code, "Adrenaline Airways" AS description FROM {USERNAME}_airlines.airlines_de t WHERE t.code = "UA") source ON s.code = source.code WHEN MATCHED AND s.description <> source.description THEN UPDATE SET s.description = source.description')
spark.sql(f"select * from {USERNAME}_airlines.airlines_de where code ='UA'").show()
```

![screenshots/lab2-10-merge-ua-after.png](screenshots/lab2-10-merge-ua-after.png)


### Table maintenance

**Compaction**: the setup loaded `flights` in the maintenance database month by month, which leaves many small files. Check them, compact them, and check again:

```bash
spark.sql(f"SELECT file_path, file_size_in_bytes/1024/1024 MB FROM {USERNAME}_airlines_maint.flights.files").show(100,False)
```

![screenshots/lab2-11-files-before.png](screenshots/lab2-11-files-before.png)

```bash
spark.sql(f"CALL spark_catalog.system.rewrite_data_files(table => '{USERNAME}_airlines_maint.flights', options => map('target-file-size-bytes','157286400'))").show()
```

![screenshots/lab2-12-compaction.png](screenshots/lab2-12-compaction.png)

```bash
spark.sql(f"SELECT file_path, file_size_in_bytes/1024/1024 MB FROM {USERNAME}_airlines_maint.flights.files order by partition").show(100,False)
```

![screenshots/lab2-13-files-after.png](screenshots/lab2-13-files-after.png)

**Partition evolution**: add `month` to the partition spec, then rewrite only 1996 with the new spec:

```bash
spark.sql(f"ALTER TABLE {USERNAME}_airlines_maint.flights add PARTITION FIELD month").show(100,False)
spark.sql(f"CALL spark_catalog.system.rewrite_data_files( table => '{USERNAME}_airlines_maint.flights' , where => 'year=1996' , options => map('rewrite-all', 'true') )").show()
```

![screenshots/lab2-15-rewrite-1996.png](screenshots/lab2-15-rewrite-1996.png)

```bash
spark.sql(f"SELECT file_path, partition, file_format, record_count, round(file_size_in_bytes/1024/1024) MB FROM {USERNAME}_airlines_maint.flights.files order by partition").show(100)
```

![screenshots/lab2-16-files-partition.png](screenshots/lab2-16-files-partition.png)

> **Note:** v1.1 set `USERNAME="user01"` in this section: keep your own user id.

**Rollback**: list the snapshots of `airlines_de` and roll back to the first one (before the MERGE):

```bash
spark.sql(f"SELECT snapshot_id FROM {USERNAME}_airlines.airlines_de.snapshots").show()
first_snapshot = spark.sql(f"SELECT snapshot_id FROM spark_catalog.{USERNAME}_airlines.airlines_de.snapshots").first()[0]
spark.sql(f"CALL spark_catalog.system.rollback_to_snapshot('{USERNAME}_airlines.airlines_de', {first_snapshot})").show()
```

![screenshots/lab2-18-rollback.png](screenshots/lab2-18-rollback.png)

**Expire snapshots** older than three days, keeping at least one:

```bash
expireDate=spark.sql("select date_sub(current_timestamp(),3) as expireDate").first()["expireDate"]
minCatalogsRetain = 1
spark.sql(f"CALL spark_catalog.system.expire_snapshots(table => '{USERNAME}_airlines.airlines_de', older_than => TIMESTAMP '{expireDate}', retain_last => {minCatalogsRetain})").show()
```

![screenshots/lab2-19-expire.png](screenshots/lab2-19-expire.png)


### Hard deletes

With time travel, deletes are soft: older snapshots still reference the deleted rows. Regulations such as the "right to be forgotten" need hard deletes. There are two ways.

**Copy-on-write (COW)**: data files are rewritten on every update or delete, so newer snapshots never reference deleted rows. Set it once per table (Spark only), then expire the older snapshots:

```bash
spark.sql(f"ALTER TABLE spark_catalog.{USERNAME}_airlines.airlines_de SET TBLPROPERTIES ('write.delete.mode'='copy-on-write', 'write.update.mode'='copy-on-write', 'write.merge.mode'='copy-on-write')").show()
```

![screenshots/lab2-21-cow-after.png](screenshots/lab2-21-cow-after.png)

```bash
spark.sql(f"CALL spark_catalog.system.expire_snapshots(table => '{USERNAME}_airlines.airlines_de', older_than => TIMESTAMP '2024-01-01 00:00:00.000', retain_last => 10)").show()
```

**Merge-on-read (MOR)**, the default for all engines: compact to remove the delete files, then expire the prior snapshots:

```bash
spark.sql(f"CALL spark_catalog.system.rewrite_data_files(table => '{USERNAME}_airlines.airlines_de', strategy => 'sort', sort_order => 'zorder(code, description)')").show()
spark.sql(f"CALL spark_catalog.system.expire_snapshots(table => '{USERNAME}_airlines.airlines_de', older_than => TIMESTAMP '2025-01-01 00:00:00.000', retain_last => 1)").show()
```

![screenshots/lab2-22-zorder.png](screenshots/lab2-22-zorder.png)

![screenshots/lab2-23-mor-expire.png](screenshots/lab2-23-mor-expire.png)


## Lab 3 - Data Warehouse with Impala

Run these statements in the Hue **Impala** editor with your user id for `${user_id}`. Select each statement (to the `;`) and run it.


### Migrate tables to Iceberg

**Create Table As Select (CTAS)** creates a new Iceberg table from an existing table:

```bash
drop table if exists ${user_id}_airlines.airports_dw;

CREATE TABLE ${user_id}_airlines.airports_dw
STORED AS ICEBERG AS
  SELECT * FROM ${user_id}_airlines.airports_hive;

DESCRIBE FORMATTED ${user_id}_airlines.airports_dw;
```

In the output, check **Location** (data in HDFS), **metadata_location** (Iceberg metadata in the table directory), **table_type** ICEBERG and the **HiveIcebergSerDe** storage handler.

![screenshots/lab3-01-ctas-airports-dw.png](screenshots/lab3-01-ctas-airports-dw.png)

**New in 7.3.2: in-place migration in Impala.** v1.1 said this was not available in Base; Impala now converts an existing (non-transactional) table without rewriting the data:

```bash
CREATE EXTERNAL TABLE ${user_id}_airlines.planes_cv STORED AS PARQUET AS SELECT * FROM airlines_csv.planes_csv;

ALTER TABLE ${user_id}_airlines.planes_cv CONVERT TO ICEBERG;

DESCRIBE FORMATTED ${user_id}_airlines.planes_cv;
```

![screenshots/lab3-11-convert-to-iceberg.png](screenshots/lab3-11-convert-to-iceberg.png)


### Create and load a partitioned Iceberg table

```bash
drop table if exists ${user_id}_airlines.flights_dw;

CREATE TABLE ${user_id}_airlines.flights_dw (
 month int, dayofmonth int,
 dayofweek int, deptime int, crsdeptime int, arrtime int,
 crsarrtime int, uniquecarrier string, flightnum int, tailnum string,
 actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int,
 depdelay int, origin string, dest string, distance int, taxiin int,
 taxiout int, cancelled int, cancellationcode string, diverted string,
 carrierdelay int, weatherdelay int, nasdelay int, securitydelay int,
 lateaircraftdelay int
)
PARTITIONED BY (year int)
STORED AS ICEBERG;

DESCRIBE EXTENDED ${user_id}_airlines.flights_dw;
```

![screenshots/lab3-02-describe-extended.png](screenshots/lab3-02-describe-extended.png)

Load the years up to 2006 (about 6 minutes, the longest step of the labs):

```bash
INSERT INTO ${user_id}_airlines.flights_dw
 SELECT * FROM airlines_csv.flights_csv
 WHERE year <= 2006;

SELECT year, count(*)
FROM ${user_id}_airlines.flights_dw
GROUP BY year
ORDER BY year desc;
```

From 2004 to 2006 there are about 7.1 million rows per year. (The screenshot below was taken after the 2007 load in the next section.)

![screenshots/lab3-03-count-by-year.png](screenshots/lab3-03-count-by-year.png)


### Partition evolution

Change the partition spec in place: no data is rewritten, existing data stays partitioned by year.

```bash
ALTER TABLE ${user_id}_airlines.flights_dw
SET PARTITION spec ( year, month );

SHOW CREATE TABLE ${user_id}_airlines.flights_dw;
```

![screenshots/lab3-04-show-create.png](screenshots/lab3-04-show-create.png)

Load 2007, which uses the new year + month spec (about 40 seconds):

```bash
INSERT INTO ${user_id}_airlines.flights_dw
 SELECT * FROM airlines_csv.flights_csv
 WHERE year = 2007;
```

Compare the plans of the same query for December 2006 (year partition) and December 2007 (year + month partitions). Prefix each query with `EXPLAIN`, or select it and use the explain option:

```bash
EXPLAIN SELECT year, month, count(*)
FROM ${user_id}_airlines.flights_dw
WHERE year = 2006 AND month = 12
GROUP BY year, month
ORDER BY year desc, month asc;
```

![screenshots/lab3-05-explain-2006.png](screenshots/lab3-05-explain-2006.png)

```bash
EXPLAIN SELECT year, month, count(*)
FROM ${user_id}_airlines.flights_dw
WHERE year = 2007 AND month = 12
GROUP BY year, month
ORDER BY year desc, month asc;
```

![screenshots/lab3-06-explain-2007.png](screenshots/lab3-06-explain-2007.png)

The 2006 query reads the whole year partition (about 7.14 million rows); the 2007 query reads only the December partition (about 614 thousand rows).


### Time travel

Every load created a snapshot. List them:

```bash
DESCRIBE HISTORY ${user_id}_airlines.flights_dw;
```

![screenshots/lab3-07-describe-history.png](screenshots/lab3-07-describe-history.png)

Enter a time between the two snapshots for `${create_ts}` and the first snapshot id for `${snapshot_id}`. Both queries return the data as it was before the 2007 load:

```bash
SELECT year, count(*)
FROM ${user_id}_airlines.flights_dw
  FOR SYSTEM_TIME AS OF '${create_ts}'
GROUP BY year
ORDER BY year desc;
```

![screenshots/lab3-08-time-travel-timestamp.png](screenshots/lab3-08-time-travel-timestamp.png)

```bash
SELECT year, count(*)
FROM ${user_id}_airlines.flights_dw
  FOR SYSTEM_VERSION AS OF ${snapshot_id}
GROUP BY year
ORDER BY year desc;
```

![screenshots/lab3-09-time-travel-snapshot.png](screenshots/lab3-09-time-travel-snapshot.png)


### Hive and Iceberg tables in one query

You can migrate to Iceberg over time: one query can join a Hive-format table (`unique_tickets`) with an Iceberg table (`flights_dw`). The query takes about 10 seconds.

```bash
SELECT
t.leg1origin,
f.dest,
count(*) as num_passengers
FROM ${user_id}_airlines.unique_tickets t
LEFT OUTER JOIN ${user_id}_airlines.flights_dw f ON
  t.leg1origin = f.origin
  AND t.leg1dest = f.dest
  AND t.leg1flightnum = f.flightnum
  AND t.leg1month = f.month
  AND t.leg1dayofmonth = f.dayofmonth
GROUP BY t.leg1origin, f.dest;
```

![screenshots/lab3-10-mixed-formats.png](screenshots/lab3-10-mixed-formats.png)


## Lab 4 - SDX: Fine-grained Access Control and Lineage


### Ranger: mask a column

Query the planes table: the tail number is plain text.

```bash
SELECT * FROM ${user_id}_airlines.planes_iceberg;
```

1. Open **Ranger** from the Knox homepage → **Hadoop SQL** → **Masking** tab → **Add New Policy**.
    ![screenshots/lab4-01-ranger-masking-tab.png](screenshots/lab4-01-ranger-masking-tab.png)

2. Policy name `<user_id>-iceberg-fgac`; Hive Database `<user_id>_airlines`; Hive Table `planes_iceberg`; Hive Column `tailnum`. Under Mask Rules: Select User your user id, **Add Permissions** → select, **Select Masking Option** → Hash.
    ![screenshots/lab4-02-ranger-mask-hash.png](screenshots/lab4-02-ranger-mask-hash.png)

    ![screenshots/lab4-03-ranger-policy-form.png](screenshots/lab4-03-ranger-policy-form.png)

3. Click **Save**.
    ![screenshots/lab4-04-ranger-policy-saved.png](screenshots/lab4-04-ranger-policy-saved.png)

4. Run the query again (Ranger plugins refresh their policies within 30 seconds): `tailnum` is now hashed.
    ![screenshots/lab4-05-masked-tailnum.png](screenshots/lab4-05-masked-tailnum.png)



### Atlas: metadata and lineage

1. Open **Atlas** from the Knox homepage. Search By Type `iceberg_table`, Search By Text `flights_dw`, click **Search**, and open `flights_dw`.
    ![screenshots/lab4-10-atlas-search.png](screenshots/lab4-10-atlas-search.png)

    ![screenshots/lab4-11-atlas-entity.png](screenshots/lab4-11-atlas-entity.png)

2. The **Schema** tab lists the columns of the Iceberg table.
    ![screenshots/lab4-12-atlas-schema.png](screenshots/lab4-12-atlas-schema.png)

3. The **Lineage** tab shows how the table was built: the CSV files in HDFS, the `flights_csv` table, and the Impala INSERT that loaded `flights_dw`.
    ![screenshots/lab4-13-atlas-lineage.png](screenshots/lab4-13-atlas-lineage.png)



## Lab 5 - Replication

Iceberg replication (Replication Manager) copies Iceberg tables between two clusters. It is not covered in v2.0 because the lab cluster is a single cluster. v1.1 describes the steps; the limits it lists still apply (HDFS to HDFS only, no Ozone tables, one source per target).


## Lab 6 - Cloudera Lakehouse Optimizer

Streaming writers commit often. In Lab 1, NiFi PutIceberg committed every record on its own, so `countries_nifi_iceberg` holds one tiny data file and one snapshot per row. Every query has to open all those files and read all that metadata. **Cloudera Lakehouse Optimizer** (new in 7.3.2) maintains Iceberg tables for you: you attach a policy to the tables, and it runs the maintenance as Spark jobs (compaction, snapshot expiration, manifest rewrite, orphan file and position delete cleanup) when the table changes or on a schedule.

Policies are built from the **ClouderaAdaptive** template, which decides from the table statistics which actions a table needs. In this lab you create a policy for the Lab 1 streaming tables, preview it with a dry run, run it on one table and compare the table before and after.

> **Note:** Use one policy per table: two policies on the same table can run at the same time and conflict. If several users share the cluster, each user creates the policy for their own `${user_id}_airlines` tables.


### Look at the problem

In the Hue **Impala** editor, count the data files, snapshots and rows of the NiFi table. The `files` and `snapshots` metadata tables are part of every Iceberg table:

```bash
SELECT 'data files' AS metric, count(*) AS value FROM ${user_id}_airlines.countries_nifi_iceberg.`files`
UNION ALL
SELECT 'snapshots', count(*) FROM ${user_id}_airlines.countries_nifi_iceberg.snapshots
UNION ALL
SELECT 'rows', count(*) FROM ${user_id}_airlines.countries_nifi_iceberg;
```

![screenshots/lab6-01-before-counts.png](screenshots/lab6-01-before-counts.png)

Each data file holds a single row of about 1.7 KB:

```bash
SELECT regexp_extract(file_path, '[^/]+$', 0) AS file_name,
       file_size_in_bytes, record_count
FROM ${user_id}_airlines.countries_nifi_iceberg.`files`
LIMIT 10;
```

![screenshots/lab6-02-before-files.png](screenshots/lab6-02-before-files.png)


### Open the Lakehouse Optimizer

Open the Lakehouse Optimizer UI through Knox: `https://<knox-host>/gateway/cdp-proxy/lakehouse-ui/` (or **Lakehouse Optimizer** on the Knox home page). The **Policies** tab lists the policies; **ClouderaAdaptive** is the default template. Click **Create Policy**.

![screenshots/lab6-03-policies-tab.png](screenshots/lab6-03-policies-tab.png)


### Create the policy

**General Information:** choose the **Namespace** scope (the policy applies to the tables you pick in a namespace; **Catalog** would apply it to every table of the chosen namespaces). Name the policy `${user_id}_airlines_small_files` and add a description. Click **Next**.

![screenshots/lab6-04-general-info.png](screenshots/lab6-04-general-info.png)

**Namespace Associations:** choose the namespace `${user_id}_airlines` and select the four Lab 1 streaming tables: `airports_kafka_iceberg`, `airports_nifi_iceberg`, `countries_kafka_iceberg` and `countries_nifi_iceberg`. Leave the other tables out: expiring snapshots on `flights_dw` would remove the snapshots that the Lab 3 time travel step uses. Click **Next**.

![screenshots/lab6-05-namespace-tables.png](screenshots/lab6-05-namespace-tables.png)

**Policy Actions:** keep the **ClouderaAdaptive** template, the **Event Based** schedule (the policy is evaluated when the table changes, which suits frequently written tables) and the **Medium** priority. Under **Expire Snapshot**, set **Expire Older Than** to `3600` (seconds, so 1 hour) and keep **Retain Last** at 5.

![screenshots/lab6-06-actions-schedule.png](screenshots/lab6-06-actions-schedule.png)

Select **Compaction** and turn on **Partial Progress Enabled** (commits the rewrite in parts, which avoids out-of-memory failures on large tables). Select **Rewrite Manifest**. Leave **Delete Orphan Files** and **Rewrite Position Delete Files** off: review those carefully before you use them, because they delete files. Click **Next**.

![screenshots/lab6-07-actions-compaction.png](screenshots/lab6-07-actions-compaction.png)

**Review** the settings and click **Finish**.

![screenshots/lab6-08-review.png](screenshots/lab6-08-review.png)

![screenshots/lab6-09-policy-created.png](screenshots/lab6-09-policy-created.png)

The **Table Associations** tab now lists your namespace and the four tables with the policy. Associating the policy declared the namespace to the Optimizer.

![screenshots/lab6-10-table-associations.png](screenshots/lab6-10-table-associations.png)

Click `countries_nifi_iceberg` and open **Table Stats**: the Optimizer reads the same numbers from the table metadata (506 data files, 506 snapshots).

![screenshots/lab6-11-table-stats-before.png](screenshots/lab6-11-table-stats-before.png)


### Dry run and run the policy

An event-based policy runs on the next write to the table. To run it now, use the Lakehouse Optimizer REST API through the Knox `cdp-proxy-api` topology. Always dry run first: the dry run evaluates the policy and only lists the actions it would take. On your laptop or the cluster host:

```bash
export USERNAME=admin
export CLO_API=https://<knox-host>/gateway/cdp-proxy-api/clo/api/v1

curl -sk -u $USERNAME -X PUT \
  "$CLO_API/policies/${user_id}_airlines_small_files/tables/${user_id}_airlines.countries_nifi_iceberg/dryrun" | jq .
```

curl asks for your password. The output lists **REWRITE_DATA_FILES** (compaction) and **EXPIRE_SNAPSHOTS** with your settings. Rewrite Manifest is not listed: ClouderaAdaptive only generates the actions that the table statistics call for.

![screenshots/lab6-13-dry-run.png](screenshots/lab6-13-dry-run.png)

> **Note:** The **Dry Run Then Execute** action in the UI (Table Associations > table > Policies > actions) works on Cloudera on cloud. On Cloudera Base 7.3.2 it does not start, so use the REST API as shown.

Run the policy on the table. The response contains the task ID:

```bash
curl -sk -u $USERNAME -X POST \
  "$CLO_API/policies/${user_id}_airlines_small_files/tables/${user_id}_airlines.countries_nifi_iceberg/evaluation" | jq .
```


### Monitor the task

The task runs as a Spark job through Livy and takes about two minutes. On the **Recent Task Activity** tab, choose your namespace and click **Refresh**; the green icon shows the completed task.

![screenshots/lab6-14-recent-task-activity.png](screenshots/lab6-14-recent-task-activity.png)

Open the task's actions menu and click **Task Details**. Compaction rewrote the 506 small files into one; snapshot expiration then removed the old snapshots and their manifests. You can also query the task with `curl -sk -u $USERNAME "$CLO_API/tasks/id/<task-id>" | jq .`

![screenshots/lab6-15-task-details.png](screenshots/lab6-15-task-details.png)

Run the counting query from the start of the lab again: one data file, five snapshots, and the same 506 rows.

```bash
SELECT 'data files' AS metric, count(*) AS value FROM ${user_id}_airlines.countries_nifi_iceberg.`files`
UNION ALL
SELECT 'snapshots', count(*) FROM ${user_id}_airlines.countries_nifi_iceberg.snapshots
UNION ALL
SELECT 'rows', count(*) FROM ${user_id}_airlines.countries_nifi_iceberg;
```

![screenshots/lab6-16-after-counts.png](screenshots/lab6-16-after-counts.png)

The **View Policy** page (click the policy on the **Policies** tab) shows the run status, the number of task runs and the last run for each table. The other three tables are maintained the next time NiFi or SSB writes to them.

![screenshots/lab6-17-view-policy.png](screenshots/lab6-17-view-policy.png)

> **Note:** Pause maintenance during bulk loads with **Pause Table** in the table's actions menu on **Table Associations**, and resume it with **Unpause Table**. The Optimizer also pauses a table by itself after repeated failures.


## Cleanup

Delete your Lakehouse Optimizer policy (**View Policy** > **Actions** > **Delete Policy**). Stop your NiFi process group and your SSB jobs, then drop your databases in the Hue Impala editor:

```bash
drop database if exists ${user_id}_airlines cascade;
drop database if exists ${user_id}_airlines_maint cascade;
```


## Appendix


### Lab cluster settings

The labs need these settings on the cluster. `lakehouse-cluster.yml` applies all of them; they are listed here for clusters built another way.

| Setting | Why |
|---|---|
| Hue `trusted_origins` = the external Knox URL | Otherwise Hue rejects every browser request through Knox ("Origin checking failed") and the editor stays blank |
| HDFS **Enable Ranger Authorization**, and a Ranger HDFS policy for the lab users and `ssb` on `/warehouse/tablespace/*/hive` | Impala creates the lab databases as impala:hive drwxrwx---; NiFi PutIceberg, Spark and SSB write to them directly |
| Ranger Hadoop SQL policy for `ssb` on `*_airlines*` | SSB registers its Iceberg tables in the Hive Metastore as the ssb user |
| Ranger NiFi and NiFi Registry policies for the lab users | The default policies only cover the nifi / nifiregistry service users |
| Flink **Checkpointing Interval** 30000 ms | The Iceberg sink only commits on checkpoints; without them SSB INSERT jobs show no rows |
| Single host: Kafka `offsets.topic.replication.factor`, `transaction.state.log.replication.factor`, `transaction.state.log.min.isr` = 1 | One broker cannot host the internal topics at replication 3; consumer groups and SSB samples hang |
| Single host: YARN NodeManager 24 GB / 12 vcores, HDFS replication 1 | Room for the SSB jobs and Spark; no under-replicated blocks |
| NiFi `nifi.web.proxy.host` includes the external Knox host | Otherwise NiFi answers 421 Invalid Proxy Host through Knox |
| Lakehouse Optimizer: Livy for Spark 3 and Metering V2 services, and the Lakehouse UI parcel | The Optimizer requires both services; Lakehouse UI is a separate parcel |
| Lakehouse Optimizer `dlm_java_opts` = `-Djavax.net.ssl.trustStore=<Auto-TLS truststore>` | Otherwise its Ranger plugin cannot reach Ranger over TLS and the Optimizer lists no namespaces |
| Lakehouse Optimizer `dlm_security_role_admin` includes the lab users' group | Otherwise the Optimizer API answers "No roles found for subject" |
| Lakehouse UI `knox_host` = the internal Knox gateway (`https://<host>:8443`) | The UI server calls the Optimizer API through Knox and cannot reach the external Knox address |


### Performance tuning with Iceberg metadata tables

Iceberg metadata tables: history, metadata_logs, snapshots, manifests, all_manifests, entries, all_entries, files, data_files, delete_files, all_files, all_data_files, all_delete_files. Useful queries (Spark SQL):

```bash
-- Files per partition
SELECT partition, file_count FROM db.table.partitions;

-- Size of each partition
SELECT partition, sum(file_size_in_bytes) AS partition_size FROM db.table.files GROUP BY partition;

-- Files added by a snapshot
SELECT data_file.file_path FROM db.table.entries WHERE snapshot_id = <id> AND status = 1;

-- Manifests: how many, and are they sorted?
SELECT count(*) FROM db.table.manifests;
SELECT path, partition_summaries FROM db.table.manifests;

-- Too many small files?
SELECT partition, count(*) AS file_count, sum(file_size_in_bytes)/count(*) AS avg_size FROM db.table.files GROUP BY partition;
```

Iceberg support by engine and form factor: [Apache Iceberg in Cloudera](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html).


### Lab setup SQL

`hol-setup.sh` runs this in Hive (`setup-admin.sql`, with `${cdp_hdfs_dir}` = `data`):

```bash
-- ADMIN RUN ON EACH CLUSTER TO SETUP DATA
CREATE DATABASE IF NOT EXISTS airlines_csv;

-- CREATE CSV TABLES
drop table if exists airlines_csv.flights_csv;

CREATE EXTERNAL TABLE airlines_csv.flights_csv (month int, dayofmonth int, dayofweek int, deptime int, crsdeptime int, arrtime int, crsarrtime int, uniquecarrier string, flightnum int, tailnum string, actualelapsedtime int, crselapsedtime int, airtime int, arrdelay int, depdelay int, origin string, dest string, distance int, taxiin int, taxiout int, cancelled int, cancellationcode string, diverted string, carrierdelay int, weatherdelay int, nasdelay int, securitydelay int, lateaircraftdelay int, year int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/${cdp_hdfs_dir}/airlines-csv/flights' tblproperties("skip.header.line.count"="1");

drop table if exists airlines_csv.planes_csv;

CREATE EXTERNAL TABLE airlines_csv.planes_csv (tailnum string, owner_type string, manufacturer string, issue_date string, model string, status string, aircraft_type string, engine_type string, year int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/${cdp_hdfs_dir}/airlines-csv/planes' tblproperties("skip.header.line.count"="1");

drop table if exists airlines_csv.airlines_csv;

CREATE EXTERNAL TABLE airlines_csv.airlines_csv (code string, description string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/${cdp_hdfs_dir}/airlines-csv/airlines/' tblproperties("skip.header.line.count"="1");

drop table if exists airlines_csv.airports_csv;

CREATE EXTERNAL TABLE airlines_csv.airports_csv (iata string, airport string, city string, state DOUBLE, country string, lat DOUBLE, lon DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/${cdp_hdfs_dir}/airlines-csv/airports' tblproperties("skip.header.line.count"="1");

drop table if exists airlines_csv.unique_tickets_csv;

CREATE external TABLE airlines_csv.unique_tickets_csv (ticketnumber BIGINT, leg1flightnum BIGINT, leg1uniquecarrier STRING, leg1origin STRING,   leg1dest STRING, leg1month BIGINT, leg1dayofmonth BIGINT,   
 leg1dayofweek BIGINT, leg1deptime BIGINT, leg1arrtime BIGINT,   
 leg2flightnum BIGINT, leg2uniquecarrier STRING, leg2origin STRING,   
 leg2dest STRING, leg2month BIGINT, leg2dayofmonth BIGINT,   leg2dayofweek BIGINT, leg2deptime BIGINT, leg2arrtime BIGINT ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE LOCATION '/${cdp_hdfs_dir}/airlines-csv/unique_tickets' 
tblproperties("skip.header.line.count"="1");
```


### Workshop repository

All lab files are in https://github.com/ylysn/lakehouse-workshop

| File | Purpose |
|---|---|
| `v2/hol-setup.sh` | Lab administrator: data download, HDFS load, user home directories, airlines_csv tables |
| `v2/setup-admin.sql` | airlines_csv tables (Hive), run by hol-setup.sh |
| `v2/setup-user-impala.sql` | Per-user databases and tables (Hue Impala editor) |
| `v2/setup-user-hive.sql` | Per-user planes table (Hue Hive editor) |
| `v2/SSBDemo-v2.json` | NiFi 2 flow definition with generic parameters (parameter context lakehouse_hol_params), based on Steven Matison's SSBDemo.json |
| `SSB-Iceberg-Demo/` | SQL Stream Builder project by Steven Matison (topic names set to the example user) |
| `guide/` | This guide (.docx and .md with screenshots) |

