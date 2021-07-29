#! /bin/bash

ALLOCATOR_ROOT=$(dirname $(readlink -f $0))/..

#export FLASK_APP=/home/di39mal/devel/flask/repos/lx_allocator/../src/allocator.py
export FLASK_APP=$ALLOCATOR_ROOT/src/allocator.py
export FLASK_DEBUG=1

#flask run --host 192.168.2.101 --port 9000 --with-threads
python3 $ALLOCATOR_ROOT/src/allocator.py > $ALLOCATOR_ROOT/logs/web.log 2>&1 &
#python3 ../src/allocator.py
