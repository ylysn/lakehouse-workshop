#!/bin/bash
# Lakehouse HOL v2 (CDP 7.3.2) - lab administrator setup, run once on the base host as a user with sudo.
# Loads the airline data into HDFS (/data/airlines-csv) and creates the airlines_csv tables (Hive).
# Lab users then run setup-user-impala.sql (Impala) and setup-user-hive.sql (Hive) in Hue.
#   ./hol-setup.sh <admin_password> [lab users...]      e.g.  ./hol-setup.sh '<admin_password>' admin user01 user02
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PASS=$1; shift; USERS=("${@:-admin}")
WORK=${WORK:-$HOME/hol}
mkdir -p "$WORK" && cd "$WORK"

# 1. Airline data: the flights file comes as a split tar archive (flights.tar.gz.aa, .ab, ...)
[ -d airlines-source-data ] || git clone --depth 1 https://github.com/jingalls1217/airlines-source-data.git
cd airlines-source-data/airlines-csv
[ -s flights/flights.csv ] || cat flights/flights.tar.gz.* | tar xzf - --to-stdout flights/flights.csv > flights/flights.csv

# 2. HDFS: /data/airlines-csv and a home directory per lab user (as the hdfs superuser)
NN_KEYTAB=$(sudo bash -c 'ls -td /var/run/cloudera-scm-agent/process/*-hdfs-NAMENODE | head -1')/hdfs.keytab
sudo -u hdfs bash -c "export KRB5CCNAME=/tmp/krb5cc_hol_hdfs; kinit -kt $NN_KEYTAB hdfs/\$(hostname -f)
  hdfs dfs -mkdir -p /data/airlines-csv && hdfs dfs -chmod -R 777 /data
  for u in ${USERS[*]}; do hdfs dfs -mkdir -p /user/\$u && hdfs dfs -chown \$u /user/\$u; done
  kdestroy"

# 3. Upload the CSV files (as the lab admin)
export KRB5CCNAME=/tmp/krb5cc_hol_admin; printf '%s\n' "$PASS" | kinit admin >/dev/null
for d in airlines airports planes unique_tickets flights; do
  hdfs dfs -mkdir -p /data/airlines-csv/$d
  hdfs dfs -put -f $d/$d.csv /data/airlines-csv/$d/
done
hdfs dfs -du -s -h '/data/airlines-csv/*'

# 4. airlines_csv tables (Hive)
sed 's/${cdp_hdfs_dir}/data/g' "$SCRIPT_DIR/setup-admin.sql" > /tmp/hol-setup-admin.sql
beeline --silent=true -f /tmp/hol-setup-admin.sql
kdestroy
