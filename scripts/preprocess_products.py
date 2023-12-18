#!/usr/bin/env python

import subprocess

conda_activate_command = 'conda activate bigdata'
subprocess.run(conda_activate_command, shell=True, executable='/bin/bash')

import pandas as pd

SOURCEDATAPATH = "/home/vagrant/project/BigDataProject/data/Products.csv"
DESTDATAPATH = "/home/vagrant/project/BigDataProject/data/fda_drugs.csv"

df = pd.read_csv(SOURCEDATAPATH, sep = ";")
df_preprocessed = df.sort_values('ProductNo').drop_duplicates(subset = 'ApplNo', keep='first')
df_preprocessed.to_csv(DESTDATAPATH, index = False)