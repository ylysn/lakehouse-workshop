# Open Data Lakehouse Hands-on Labs

Lab files for the **CDP 7.3.2 Lakehouse Hands-on Labs v2.0** (Iceberg with NiFi, SQL Stream Builder,
Spark, Impala, Ranger and Atlas on Cloudera on premises 7.3.2).

**Lab guide:** [`guide/CDP 7.3.2 Lakehouse - HOL v2.0.md`](guide/CDP%207.3.2%20Lakehouse%20-%20HOL%20v2.0.md)
(also as `.docx` in `guide/`).

| Path | Used by | Purpose |
|---|---|---|
| `v2/hol-setup.sh` | lab administrator, on the cluster base host | Loads the airline data into HDFS, creates user home directories and the `airlines_csv` tables |
| `v2/setup-admin.sql` | `hol-setup.sh` | `airlines_csv` tables (Hive) |
| `v2/setup-user-impala.sql` | each lab user, Hue Impala editor | User databases and tables |
| `v2/setup-user-hive.sql` | each lab user, Hue Hive editor | The `planes` table (Hive) |
| `v2/SSBDemo-v2.json` | each lab user, NiFi | NiFi 2 flow: routes, countries and airports to Kafka and Iceberg |
| `SSB-Iceberg-Demo/` | each lab user, SQL Stream Builder Import | SSB project: Kafka virtual tables and Iceberg jobs |
| `guide/` | everyone | The lab guide (.md and .docx) and its screenshots |

## Lab administrator setup

```bash
git clone https://github.com/ylysn/lakehouse-workshop.git
cd lakehouse-workshop/v2
./hol-setup.sh '<admin password>' admin user01 user02
```

The airline data comes from [jingalls1217/airlines-source-data](https://github.com/jingalls1217/airlines-source-data).

## Credits

The NiFi flow and the SQL Stream Builder project were created by **Steven Matison** (Cloudera)
for the CDP 7.1.9 Lakehouse Hands-on Lab:

- NiFi flow: [cldr-steven-matison/NiFi-Templates](https://github.com/cldr-steven-matison/NiFi-Templates) (`SSBDemo.json`)
- SSB project: [cldr-steven-matison/SSB-Iceberg-Demo](https://github.com/cldr-steven-matison/SSB-Iceberg-Demo)

v2 adapts them for CDP 7.3.2: the flow is exported from NiFi 2.11 (CFM 4.13) with generic parameters,
and the SSB project's Kafka topics are set to the lab's example user.
