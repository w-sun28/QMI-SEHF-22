from datetime import date
from sqlite3 import Row
from tokenize import String
import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup
from collections import defaultdict
import re
import math

ticker = 'VGT'
print(ticker + " is being printed")
URLRequest_STMR = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1=1325376000&period2=1609459200&interval=1d&events=history&includeAdjustedClose=true"
unclean_df_STMR = pd.read_csv(URLRequest_STMR)
unclean_df_STMR.drop(["Open", "High", "Low", "Close", "Volume"], axis=1, inplace=True)
natural_log_df = pd.DataFrame(columns = ['Date', 'NL Values'])
final_df = pd.DataFrame(columns = ['Date', 'STMR'])
for index, row in unclean_df_STMR[1:].iterrows():
    date = unclean_df_STMR.loc[index]['Date']
    temp = (row[1:2])
    temp = temp.astype(float)
    temp_30 = unclean_df_STMR.iloc[index - 1];
    temp_30_clean = (temp_30[1:2])
    temp_30_clean = temp_30_clean.astype(float)
    nl_val = (temp/temp_30)
    nl_val = nl_val[0:1]
    nl_val = math.log(nl_val)
    temp_list = {'Date': date, 'NL Values': nl_val} 
    temp_list = pd.DataFrame([temp_list])
    natural_log_df = pd.concat([natural_log_df, temp_list], ignore_index=True)
#print(natural_log_df)
print(len(natural_log_df))

# Revised Short-Term Mean Reversion
    # Create for loop beginning at x
for index, row in natural_log_df[753:].iterrows():
    date = unclean_df_STMR.loc[index]['Date']
    thirty_day_avg = 0.0;
    natural_log_df.loc[index]['NL Values']
    for i in range(20):
        temp_day_val = natural_log_df.loc[index - 20 + i]['NL Values']
        thirty_day_avg += temp_day_val
    #print(thirty_day_avg)
    temp_list = {'Date': date, 'STMR': thirty_day_avg}
    temp_list = pd.DataFrame([temp_list])
    #print(temp_list)
    final_df = pd.concat([final_df, temp_list], ignore_index=True)
final_df.reset_index()
#print(final_df)     

    # Create a nested for loop that iterates thirty times for previous days, store that value
    # Add to the appropriate date, probably should use loc to match up w the date
    # Create a new column, and append value as a separate data frame

# Revised MOMENTUM
momentum_column = pd.DataFrame(columns=['momentum'])
for index, row in natural_log_df[753:].iterrows():
    date = unclean_df_STMR.loc[index]['Date']
    thirty_day_avg = final_df.loc[index - 753]['STMR']
    one_year_avg = 0.0;
    natural_log_df.loc[index]['NL Values']
    for i in range(252):
        temp_day_val = natural_log_df.loc[index - 252 + i]['NL Values']
        one_year_avg += temp_day_val
    momentum_val = one_year_avg/thirty_day_avg
    temp_list = {'momentum': momentum_val}
    temp_list = pd.DataFrame([temp_list])
    #print(temp_list)
    momentum_column = pd.concat([momentum_column, temp_list], ignore_index=True)
final_df = final_df.join(momentum_column)
#print(final_df)

# Revised LTMR
ltmr_column = pd.DataFrame(columns=['ltmr'])
for index, row in natural_log_df[753:].iterrows():
    date = unclean_df_STMR.loc[index - 753]['Date']
    one_year_avg = 0.0;
    four_year_avg = 0.0
    natural_log_df.loc[index]['NL Values']
    for i in range(753):
        temp_day_val = natural_log_df.loc[index - 753 + i]['NL Values']
        four_year_avg += temp_day_val
    for i in range(252):
        temp_day_val = natural_log_df.loc[index - 252 + i]['NL Values']
        one_year_avg += temp_day_val
    ltmr_val = four_year_avg/one_year_avg
    temp_list = {'ltmr': ltmr_val}
    temp_list = pd.DataFrame([temp_list])
    ltmr_column = pd.concat([ltmr_column, temp_list], ignore_index=True)
final_df = final_df.join(ltmr_column)
final_df.to_csv(f'{ticker}_CSV')
    # Create for loop beginning at x
    # Create a nested for loop that iterates through 4 years, over one year, store that value
    # Add to the appropriate date, probably should use loc to match up w the date
    # Create a new column, and append value as a separate data frame

# Momentum
    # Create for loop beginning at x
    # Create a nested for loop that iterates through 1 years, over one month, store that value
    # Add to the appropriate date, probably should use loc to match up w the date
    # Create a new column, and append value as a separate data frame

# Size
    # Need to scrape a new URL (Chart URL)
    # Scrape for the last 2 years should be fine
    # Create a new column and use loc to match it up

# Market Cap
    # Same thing with the chart
    # Might have to pad for each quarter

# Short-Term Mean Reversion
"""for index, row in unclean_df_STMR[20:].iterrows():
    date = unclean_df_STMR.loc[index]['Date']
    temp = (row[1:2])
    temp = temp.astype(float)
    temp_30 = unclean_df_STMR.iloc[index - 20];
    temp_30_clean = (temp_30[1:2])
    temp_30_clean = temp_30_clean.astype(float)
    stmr_val = (temp/temp_30)
    stmr_val = stmr_val[0:1]
    stmr_val = math.log(stmr_val)
    temp_list = {'Date': date, 'STMR': stmr_val} 
    temp_list = pd.DataFrame([temp_list])
    print(temp_list)
    final_df = pd.concat([final_df, temp_list], ignore_index=True)
    break;
final_df.reset_index()"""

