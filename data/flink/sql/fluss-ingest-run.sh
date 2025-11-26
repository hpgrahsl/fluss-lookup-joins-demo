#!/bin/bash

${FLINK_HOME}/bin/sql-client.sh  -i /opt/sql-client/sql/fluss_ingest_ddl.sql -f /opt/sql-client/sql/fluss_ingest_dml.sql
