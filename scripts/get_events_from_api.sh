#!/bin/bash

# Set Hadoop environment variables
export HADOOP_HOME="/usr/local/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$PATH:$HADOOP_HOME/bin"

start_date="20230101" # Change this to yorur desired start date (data available from 19390101)

end_date=$(date +"%Y%m%d")
current_date="$start_date"

# Loop through dates until the end_date is reached
while [ "$current_date" -lt "$end_date" ]; do
    url="https://api.fda.gov/drug/event.json?api_key=${OPENFDA_API_KEY}&search=receivedate:\[${current_date}+TO+${current_date}\]&sort=receivedate:asc&limit=1000"

    response=$(curl -s -k --compressed -X GET "$url" | jq '{ results: .results }')

    if [ -n "$response" ]; then
        hdfs_path="/user/vagrant/project/nifi_in/api/${current_date}.json"
        echo "$response" | hdfs dfs -put - "$hdfs_path"
    fi

    # Increment current_date by 1 day
    current_date=$(date -d "$current_date + 1 day" +%Y%m%d)
done
