#!/usr/bin/env bash
cd /ycsb-0.16.0
CUR_RECORDS=`/opt/mssql-tools/bin/sqlcmd -C -S 10.120.18.226 -U sa -P Nutanix#4u -d SqlNos2Single01 -Q 'SELECT COUNT(*) FROM usertable'| sed -n '3 p' | tr -d ' '`
echo "Current records is usertable : ${CUR_RECORDS}"
if [ "${CUR_RECORDS}" != "10000000" ]; then
  echo "usertable already created and the prefill is not complete, delete and recreate the table"
  echo "Run: /opt/mssql-tools/bin/sqlcmd -C -S 10.120.18.226 -U sa -P Nutanix#4u -d SqlNos2Single01 -Q 'DROP TABLE usertable'..."
  /opt/mssql-tools/bin/sqlcmd -C -S 10.120.18.226 -U sa -P Nutanix#4u -d SqlNos2Single01 -Q 'DROP TABLE usertable'
  echo "Run: /opt/mssql-tools/bin/sqlcmd -C -S 10.120.18.226 -U sa -P Nutanix#4u -d SqlNos2Single01 -Q 'CREATE TABLE usertable (YCSB_KEY varchar(255) NOT NULL,FIELD0 varchar(100) NOT NULL,FIELD1 varchar(100) NOT NULL,FIELD2 varchar(100) NOT NULL,FIELD3 varchar(100) NOT NULL,FIELD4 varchar(100) NOT NULL,FIELD5 varchar(100) NOT NULL,FIELD6 varchar(100) NOT NULL,FIELD7 varchar(100) NOT NULL,FIELD8 varchar(100) NOT NULL,FIELD9 varchar(100) NOT NULL CONSTRAINT pk_usertable PRIMARY KEY (YCSB_KEY))'..."
  /opt/mssql-tools/bin/sqlcmd -C -S 10.120.18.226 -U sa -P Nutanix#4u -d SqlNos2Single01 -Q 'CREATE TABLE usertable (YCSB_KEY varchar(255) NOT NULL,FIELD0 varchar(100) NOT NULL,FIELD1 varchar(100) NOT NULL,FIELD2 varchar(100) NOT NULL,FIELD3 varchar(100) NOT NULL,FIELD4 varchar(100) NOT NULL,FIELD5 varchar(100) NOT NULL,FIELD6 varchar(100) NOT NULL,FIELD7 varchar(100) NOT NULL,FIELD8 varchar(100) NOT NULL,FIELD9 varchar(100) NOT NULL CONSTRAINT pk_usertable PRIMARY KEY (YCSB_KEY))'
  echo "Load data: ./bin/ycsb load jdbc -s -P workloads/workloada -P /root/jdbc_db_conn.txt -cp mssql-jdbc-12.6.1.jre11.jar -p recordcount=10000000 -p threadcount=5"
  ./bin/ycsb load jdbc -s -P workloads/workloada -P /root/jdbc_db_conn.txt -cp mssql-jdbc-12.6.1.jre11.jar -p recordcount=10000000 -p threadcount=5
else
  echo "SKIP loading the data, database already container [10000000] rows."
fi
echo "start run: ./bin/ycsb run  jdbc -s -P workloads/workloada -P /root/jdbc_db_conn.txt -cp mssql-jdbc-12.6.1.jre11.jar -p recordcount=10000000 -p target=10 -p operationcount=10000000"
while true
do
  ./bin/ycsb run  jdbc -s -P workloads/workloada -P /root/jdbc_db_conn.txt -cp mssql-jdbc-12.6.1.jre11.jar -p recordcount=10000000 -p target=10 -p operationcount=10000000
done