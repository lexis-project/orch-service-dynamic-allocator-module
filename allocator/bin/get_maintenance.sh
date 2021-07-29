#! /bin/bash

cluster=$1
if [[ $cluster == "ALL" ]]
then
    influx -execute "SELECT * FROM systemMaintenance" -database="lxm_maintenance" | tail -n +4 &> ../dbs/lxm_cleanup_date.txt
else
    influx -execute "SELECT * FROM systemMaintenance where cluster='${cluster}'" -database="lxm_maintenance" | tail -n +4 &> ../dbs/lxm_maintenance_date.txt
fi