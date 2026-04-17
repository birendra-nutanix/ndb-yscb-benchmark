#!/usr/bin/env bash
TIMESTAMP=`date +"%Y%m%d_%H%M%S"`
LOG_FILE="/tmp/ycsb_jdbc_$TIMESTAMP"

OS=`uname`
if [ "$OS" == "Linux" ]; then
  YCSB_FOLDER="/ycsb-0.16.0"
  YCSB_BIN="./bin/ycsb"
  KILL_CMD="pkill -f start_ycsb_jdbc.sh;pkill -f build_run_ycsb_jdbc.sh; pkill -f /ycsb"
  AUTO_START_FILE="/etc/rc.local"
  SQL_CMD_PATH="/opt/mssql-tools/bin"
else
  # TODO
  YCSB_BIN="/PostgresSQL/bin/ycsb.exe"
  KILL_CMD="taskkill /F /IM ycsb.exe"
  AUTO_START_FILE="/auto_start.sh"
fi

# Connection Args
DB_SERVER=""
DB_USER="admin"
DB_PWD="Nutanix#4u"
DB_PORT="27017"

DB_CONN_FILE="/root/jdbc_db_conn.txt"
CLASS_PATH=""

# Common Args for build and run
# DB Driver, mogondb-async, mongodb, jdbc
DRIVER="jdbc"
# Workload, workloada, workloadb, workloadc, tsworkload
# -P workload/<WORKLOAD>
WORKLOAD="workloada"
# Number of client threads (default: 1)
USERS=16

# Build schema args
# the number of records in the dataset at the start of the workload. used when loading for all workloads. (default: 1000)
# 84,000,000 records is about 100GB of data
RECORDS=84000000
# The TPS for prefill, 0 means unthrottled
PREFILL_TPS=0

# the number of operations to perform in the workload (default: 1000)
TRANSACTIONS=0
# Target ops/sec (default: unthrottled)
TPS=10


function get_options() {
  options=$(getopt -o e::s::o:: -l db_type::,db_server::,db_port::,database::,db_instance::,db_username::,db_password::,driver::,workload::,records::,prefill_tps::,users::,tps::,duration:: -- "$@")
  eval set -- "$options"
  while true; do
    case "$1" in
      # Db connection
      --db_type)      DB_TYPE=$2; shift ;;
      --db_server)    DB_SERVER=$2; shift ;;
      --db_port)      DB_PORT=$2; shift ;;
      --database)     DB_NAME=$2; shift ;;
      --db_instance)  DB_INSTANCE=$2; shift ;;
      --db_username)  DB_USER=$2; shift ;;
      --db_password)  DB_PWD=$2; shift ;;

      # Common Args
      --driver)       DRIVER=$2; shift ;;
      --workload)     WORKLOAD=$2; shift ;;


      # init args
      --records)      RECORDS=$2; shift ;;
      --prefill_tps)  PREFILL_TPS=$2; shift ;;

      # run args
      --users)        USERS=$2; shift ;;
      --tps)          TPS=$2; shift ;;
      --) shift; break ;;
    esac
    shift
  done
  TRANSACTIONS=$RECORDS
  DB_NAME_LOWER=`echo "$DB_NAME" | awk '{print tolower($0)}'`
}

function create_connection() {
  case "${DB_TYPE}" in
    sqlserver_database)
      echo "db.driver=com.microsoft.sqlserver.jdbc.SQLServerDriver
db.url=jdbc:sqlserver://${DB_SERVER};databaseName=${DB_NAME};encrypt=false;trustServerCertificate=true;sendStringParametersAsUnicode=false;disableStatementPooling=false;statementPoolingCacheSize=10;responseBuffering=full;
db.user=${DB_USER}
db.passwd=${DB_PWD}
db.batchsize=1000
jdbc.batchupdateapi=true" >${DB_CONN_FILE}
      CLASS_PATH="mssql-jdbc-12.6.1.jre11.jar"
      CMD_PREX="${SQL_CMD_PATH}/sqlcmd -C -S ${DB_SERVER} -U ${DB_USER} -P ${DB_PWD} -d ${DB_NAME}"

      # sqlcmd -S 10.52.29.184 -U sa -P Nutanix#4u -d SQL01 -C -Q "select count(*) from usertable"
      # sqlcmd -S 10.52.29.184 -U sa -P Nutanix#4u -d SQL01 -C -Q "drop table usertable"
      # sqlcmd -S 10.52.29.184 -U sa -P Nutanix#4u -d SQL01 -C -Q "CREATE TABLE usertable (YCSB_KEY varchar(255) NOT NULL,FIELD0 varchar(100) NOT NULL,FIELD1 varchar(100) NOT NULL,FIELD2 varchar(100) NOT NULL,FIELD3 varchar(100) NOT NULL,FIELD4 varchar(100) NOT NULL,FIELD5 varchar(100) NOT NULL,FIELD6 varchar(100) NOT NULL,FIELD7 varchar(100) NOT NULL,FIELD8 varchar(100) NOT NULL,FIELD9 varchar(100) NOT NULL CONSTRAINT pk_usertable PRIMARY KEY (YCSB_KEY))"

      CMD_CREATE_TABLE="${CMD_PREX} -Q 'CREATE TABLE usertable (YCSB_KEY varchar(255) NOT NULL,FIELD0 varchar(100) NOT NULL,FIELD1 varchar(100) NOT NULL,FIELD2 varchar(100) NOT NULL,FIELD3 varchar(100) NOT NULL,FIELD4 varchar(100) NOT NULL,FIELD5 varchar(100) NOT NULL,FIELD6 varchar(100) NOT NULL,FIELD7 varchar(100) NOT NULL,FIELD8 varchar(100) NOT NULL,FIELD9 varchar(100) NOT NULL CONSTRAINT pk_usertable PRIMARY KEY (YCSB_KEY))'"
      CMD_DROP_TABLE="${CMD_PREX} -Q 'DROP TABLE usertable'"
      CMD_COUNT_ROWS="${CMD_PREX} -Q 'SELECT COUNT(*) FROM usertable'| sed -n '3 p' | tr -d ' '"
        ;;
    mysql_database)
        ;;
    mariadb_database)
        ;;
    postgres_database)
      echo "db.driver=org.postgresql.Driver
db.url=jdbc:postgresql://${DB_SERVER}:${DB_PORT}/${DB_NAME_LOWER}
db.user=${DB_USER}
db.passwd=${DB_PWD}
db.batchsize=5000
jdbc.batchupdateapi=true" >${DB_CONN_FILE}
      CLASS_PATH="postgresql-42.7.1.jar"

      CMD_PREX="psql postgresql://${DB_USER}:${DB_PWD}@${DB_SERVER}/${DB_NAME_LOWER}"
      CMD_CREATE_TABLE="${CMD_PREX} -c 'CREATE TABLE usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY,FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT)'"
      CMD_DROP_TABLE="${CMD_PREX} -c 'DROP TABLE usertable'"
      CMD_COUNT_ROWS="${CMD_PREX} -c 'SELECT COUNT(*) FROM usertable'| sed -n '3 p' | tr -d ' '"

      # psql postgresql://postgres:Nutanix#4u@10.52.16.212/pg01 -c "CREATE TABLE usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY,FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT)"
      # psql postgresql://postgres:Nutanix#4u@10.52.16.212/pg01 -c 'SELECT count(*) from usertable' | sed -n '3 p' | tr -d ' '
      # psql postgresql://postgres:Nutanix#4u@10.52.16.212/pg01 -c 'DROP TABLE usertable'
        ;;
    oracle_database)
        ;;
  esac
}



function generate_build_run_script() {
  load_cmd="$YCSB_BIN load ${DRIVER} -s -P workloads/${WORKLOAD} -P ${DB_CONN_FILE} -cp ${CLASS_PATH} -p recordcount=${RECORDS} -p threadcount=${USERS}"
  if [ "${PREFILL_TPS}" != "0" ]; then
    load_cmd="$load_cmd -p target=$PREFILL_TPS"
  fi

  # run_cmd="$YCSB_BIN run  $DRIVER -s -P workloads/$WORKLOAD -p mongodb.url=mongodb://$DB_USER:$DB_PWD@$DB_SERVER:$DB_PORT/$DB_NAME?authSource=admin -p recordcount=$RECORDS -p target=$TPS"
  run_cmd="$YCSB_BIN run  $DRIVER -s -P workloads/$WORKLOAD -P ${DB_CONN_FILE} -cp ${CLASS_PATH} -p recordcount=$RECORDS -p target=$TPS"
  if [ "${TRANSACTIONS}" != "0" ]; then
    run_cmd="$run_cmd -p operationcount=$TRANSACTIONS"
  fi


  echo "#!/usr/bin/env bash
cd $YCSB_FOLDER
CUR_RECORDS=\`${CMD_COUNT_ROWS}\`
echo \"Current records is usertable : \${CUR_RECORDS}\"
if [ \"\${CUR_RECORDS}\" != \"${RECORDS}\" ]; then
  echo \"usertable already created and the prefill is not complete, delete and recreate the table\"
  echo \"Run: $CMD_DROP_TABLE...\"
  $CMD_DROP_TABLE
  echo \"Run: $CMD_CREATE_TABLE...\"
  $CMD_CREATE_TABLE
  echo \"Load data: $load_cmd\"
  $load_cmd
else
  echo \"SKIP loading the data, database already container [${RECORDS}] rows.\"
fi
echo \"start run: $run_cmd\"
while true
do
  $run_cmd
done
" >$BUILD_RUN_FILE
  chmod +x $BUILD_RUN_FILE
}

function genreate_start_script() {
  run_cmd="$YCSB_BIN run  $DRIVER -s -P workloads/$WORKLOAD -p mongodb.url=mongodb://$DB_USER:$DB_PWD@$DB_SERVER:$DB_PORT/$DB_NAME?authSource=admin -p recordcount=$RECORDS -p target=$TPS"
  if [ "${TRANSACTIONS}" != "0" ]; then
    run_cmd="$run_cmd -p operationcount=$TRANSACTIONS"
  fi
  echo "#!/usr/bin/env bash
cd $YCSB_FOLDER
while true
do
  $run_cmd
done
" >$START_FILE
  chmod +x $START_FILE
}

function config_auto_start() {
  if [ -f $AUTO_START_FILE.orig ]; then
    cp -f $AUTO_START_FILE.orig $AUTO_START_FILE
    echo "#!/bin/bash" >> $AUTO_START_FILE
    echo "nohup $START_FILE >>$LOG_FILE" >> $AUTO_START_FILE
    chmod +x $AUTO_START_FILE
  else
    cp $AUTO_START_FILE $AUTO_START_FILE.orig
    systemctl enable rc-local.service
    echo "nohup $START_FILE >>$LOG_FILE" >> $AUTO_START_FILE
  fi
}


get_options "$@"
create_connection

START_FILE="$PWD/build_run_ycsb_jdbc.sh"
BUILD_RUN_FILE="$PWD/build_run_ycsb_jdbc.sh"


generate_build_run_script
# Since build script will check whether the DB is loaded, so we can use the same script to run workload.
# genreate_start_script
config_auto_start

# $KILL_CMD will fail, so hardcoding the kill command
# $KILL_CMD
pkill -f start_ycsb_jdbc.sh;pkill -f build_run_ycsb_jdbc.sh; pkill -f /ycsb
nohup $BUILD_RUN_FILE >>$LOG_FILE 2>&1 &
root@ubuntu20:~#