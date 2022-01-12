# LEXIS DYNAMIC ALLOCATION MODULE (DAM)
This is a LEXIS infrastructure module used to dynamically manage the tasks of a running workflow. Each task is dynamically allocated to the best machine in the LEXIS resource pool, considering various performance criteria (load on the machines, number of used cores, etc.). The allocation olicy is based on a greedy strategy for ranking and selecting the best machine(s) at a given point in time. The allocator is embedded in a web-based backend service developed around the Flask framework. 

## Application description
This is a Flask-based web application that is intended to provide a simple APIs to compute and retrieve the best location(s) for a task. The result is stored in a dedicated database through a running InfluxDB service (on the same hosting machine).

## Requirements
Before proceeding to installation, an InfluxDB service must be installed on the machine. You can check this [link](https://docs.influxdata.com/influxdb/v2.0/install/?t=Linux).
Also python3 is a strict requirement.

## Installation and deployment
First install the dependencies as follows:

```
$ pip3 install -r requirements.txt
```

Finally, modify the config file ./allocator/config/lxm.conf with the relevant parameters and run the allocator:

```
$ cd allocator/bin
$ ./run.sh
```

# ACKNOWLEDGEMENT
This work was supported by the LEXIS project funded by the EU’s Horizon 2020 research and innovation programme (2014-2020) under grant agreement No 825532.