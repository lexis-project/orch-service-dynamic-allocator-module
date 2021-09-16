#! /bin/bash

#lsof -ti tcp:9000 | xargs kill
kill -9 $(lsof -i:9500 -t) 2> /dev/null
