#!/bin/bash

. /home/vagrant/miniconda3/etc/profile.d/conda.sh
conda activate bigdata

sudo /usr/local/hbase/bin/hbase thrift

python /home/vagrant/project/BigDataProject/scripts/preprocess_api_drug_events.py
