#!/bin/bash

. /home/vagrant/miniconda3/etc/profile.d/conda.sh

conda activate bigdata

python /home/vagrant/project/BigDataProject/integration/hive/preprocess_products.py