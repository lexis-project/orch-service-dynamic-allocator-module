#! /bin/bash

ALLOCATOR_ROOT=$(dirname $(readlink -f $0))/..
influxd backup -portable -database $1 $ALLOCATOR_ROOT/dbs/_$1_dump 
