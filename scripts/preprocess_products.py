import pandas as pd

SOURCEDATAPATH = "/home/vagrant/project/BigDataProject/data/Products.csv"
DESTDATAPATH = "/home/vagrant/project/BigDataProject/data/fda_drugs.csv"

df = pd.read_csv(SOURCEDATAPATH, sep = ";")

df_filtered = df.sort_values('ProductNo').drop_duplicates(subset = 'ApplNo', keep='first')

df_filtered.to_csv(DESTDATAPATH, index=False)