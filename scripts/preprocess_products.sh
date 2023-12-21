#!/bin/bash

. /home/vagrant/anaconda3/etc/profile.d/conda.sh
conda activate bigdata

python3.9 /home/vagrant/project/BigDataProject/scripts/preprocess_products.py