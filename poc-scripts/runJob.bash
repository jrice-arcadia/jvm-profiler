#!/bin/bash


while [ true ]
  do
    echo -e "\nExecuting script..."
    ./query-dcos.sh
    echo -e "\nGoing to sleep."
    sleep 1200 # 1200 seconds = 20 minutes
done
