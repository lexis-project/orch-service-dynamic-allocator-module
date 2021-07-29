# LEXIS DYNAMIC TASK ALLOCATOR
This is a LEXIS infrastructure module used to dynamically manage the tasks of a running workflow. Each task is dynamically allocated to the best machine in the LEXIS resource pool, considering various performance criteria (load on the machines, number of used cores, etc.). The allocation olicy is based on a greedy strategy for ranking and selecting the best machine(s) at a given point in time. The allocator is embedded in a web-based backend service developed around the Flask framework. 

## Application description
This is a Flask-based web application that is intended to provide a simple API (just a couple of end points) to load and retrieve data from a running InfluxDB service (on the same hosting VM).

## Development details
To develop the application, it is required to launch the virtual environment with the Flask installation.
So run the virtual environment with the Flask installation as follows:

[LINKS Saturn workstation] $ conda activate /home/saturn/Devel/flask/venv

[LRZ cloud-hpc virtual machine] $ source /devel/flask/venv/bin/activate
