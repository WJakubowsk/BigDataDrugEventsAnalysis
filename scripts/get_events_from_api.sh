#!/bin/bash

start_date="19390101"

end_date=$(date +"%Y%m%d")

current_date=$start_date

echo $end_date

while [ $start_date -lt $end_date ]; do
    filename="/home/vagrant/project/BigDataProject/data/api/${current_date}.json"

    url="https://api.fda.gov/drug/event.json?api_key=SYDGOVhKmq00Zdk2CI1bedQaki7xf4bsiGR8ZDVo&search=receivedate:\[${current_date}+TO+${current_date}\]&limit=1000"

    curl -k --compressed -X GET "$url" -o "$filename"

    current_date=$(date -d "$current_date + 1 day" +%Y%m%d)
done

