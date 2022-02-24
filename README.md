# orch-service-dynamic-allocator-module

<a href="https://doi.org/10.5281/zenodo.6080480"><img src="https://zenodo.org/badge/DOI/10.5281/zenodo.6080480.svg" alt="DOI"></a>

This submodule repository contains the LEXIS DYNAMIC ALLOCATION MODULE (DAM), a LEXIS infrastructure component used to dynamically manage the tasks of a running workflow. Each task is dynamically allocated to the best machine in the LEXIS resource pool, considering various performance criteria (load on the machines, number of used cores, etc.). The allocation olicy is based on a greedy strategy for ranking and selecting the best machine(s) at a given point in time. The allocator is embedded in a web-based backend service developed around the Flask framework. 

## Acknowledgement
This code repository is a result / contains results of the LEXIS project. The project has received funding from the European Union’s Horizon 2020 Research and Innovation programme (2014-2020) under grant agreement No. 825532.

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

