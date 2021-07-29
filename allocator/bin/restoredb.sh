#! /bin/bash

ALLOCATOR_ROOT=$(dirname $(readlink -f $0))/..

# first delete the database to be restored
influx -execute 'drop database '$1
# restore the database
influxd restore -db $1 -portable $ALLOCATOR_ROOT/dbs/_$1_dump 

