#! /bin/bash

center_src=$1
center_dst=$2
if [[ $src_center == "ALL" ]]
then
    influx -execute "SELECT * FROM networkEvaluation" -database="lxm_ddi_performance" | tail -n +4 &> ../dbs/lxm_cleanup_speed.txt
else
    influx -execute "SELECT * FROM networkEvaluation where center_src='${center_src}' and center_dst='${center_dst}'" -database="lxm_ddi_performance" | tail -n +4 &> ../dbs/lxm_speed_perf.txt
fi