#!/bin/bash

. /home/vagrant/anaconda3/etc/profile.d/conda.sh
conda activate bigdata

python /home/vagrant/project/BigDataProject/scripts/preprocess_products.py