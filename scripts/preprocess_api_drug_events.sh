#!/bin/bash

. /home/vagrant/anaconda3/etc/profile.d/conda.sh
conda activate bigdata

. /usr/local/hbase/bin/hbase thrift

python3.9 /home/vagrant/project/BigDataProject/scripts/preprocess_api_drug_events.py
