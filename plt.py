import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plot
import matplotlib.dates as mdates


# Create a DataFrame instance

df = pd.read_csv('hist.csv',sep=',', engine='python')
for i, col in enumerate(df.columns):
    df.iloc[:, i] = df.iloc[:, i].str.replace('"', '').str.replace(',', '')
df['Date'] = pd.to_datetime(df['Dt'], format='%m/%d/%y')
new_df = df[['Date', 'Price']].copy().set_index('Date')
new_df.astype(float)
new_df.Price=pd.to_numeric(new_df.Price)
#Draw an area plot for the DataFrame data

new_df.plot(kind='line', stacked=False)
plot.ylim((1600,2300))
plot.show(block=True)