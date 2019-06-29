#!/usr/bin/env bash
curl -X POST http://127.0.0.1:8100/api/namedjob/create --silent --data @conf/namedJob-templateWithAdaptiveScaling -vvv
sleep 5 
curl -X POST http://127.0.0.1:8100/api/submit --silent --data @conf/submitJob-templateWithAdaptiveScaling -vvv
