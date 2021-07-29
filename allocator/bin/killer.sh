#! /bin/bash

#lsof -ti tcp:9000 | xargs kill
kill -9 $(lsof -i:9000 -t) 2> /dev/null
