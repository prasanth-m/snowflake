import awswrangler as wr
import getpass
#iam_role = getpass.getpass()
import pandas as pd
import os

df = pd.read_csv('hist.csv',sep=',', engine='python')

for i, col in enumerate(df.columns):
    df.iloc[:, i] = df.iloc[:, i].str.replace('"', '')

df['Date'] = pd.to_datetime(df['Dt'], format='%m/%d/%y')
df['year'] = df['Date'].dt.year
df['month'] = df['Date'].dt.month
df['day'] = df['Date'].dt.day
print(df)
path = f"s3://<XXXX>/test/data/"

wr.s3.to_parquet(
    df=df,
    path=path,
    dataset=True,
    mode="overwrite",
    partition_cols=["year","month","day"],
);