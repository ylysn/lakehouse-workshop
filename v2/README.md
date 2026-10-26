# Lakehouse HOL v2 (CDP 7.3.2 SP2)

Tested on Cloudera Manager 7.13.2, Runtime 7.3.2 SP2, CSA 1.18 (Flink 1.20.5, SSB) and CFM 4.13 (NiFi 2.11).

`SSBDemo-v2.json` is based on Steven Matison's `SSBDemo.json`
([cldr-steven-matison/NiFi-Templates](https://github.com/cldr-steven-matison/NiFi-Templates)), exported
from NiFi 2.11. Its parameter context `lakehouse_hol_params` has placeholders (`<user_id>`, `<base-host>`)
that each user sets after importing; the Kafka processors already use the Default Truststore-only NiFi
SSL Context Service.
