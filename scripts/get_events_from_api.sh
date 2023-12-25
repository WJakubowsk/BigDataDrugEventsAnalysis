#!/bin/bash

export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

start_date="20220101" #"19390101 is the beginning of the data, replace it if you have enough space ond disk"

end_date=$(date +"%Y%m%d")

current_date=$start_date

while [ $current_date -lt $end_date ]; do
    url="https://api.fda.gov/drug/event.json?api_key=SYDGOVhKmq00Zdk2CI1bedQaki7xf4bsiGR8ZDVo&search=receivedate:\[${current_date}+TO+${current_date}\]&sort=receivedate:asc&limit=1000"

    response=$(curl -s -k --compressed -X GET "$url" | jq '{ results: .results }')

    if [ ! -z "$response" ]; then
        echo "$response" | hdfs dfs -put - "/user/vagrant/project/nifi_in/api/${current_date}.json"
    fi

    current_date=$(date -d "$current_date + 1 day" +%Y%m%d)
done
