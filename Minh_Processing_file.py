# %%
import pandas as pd
import os
import sqldf
import mysql
import ast
import cursor
import mysql.connector
import re

# %%
path = os.getcwd()

# %%
folder = os.listdir(path)

# %%
file_name = folder[1:-1]

# %%
file_name

# %%
for file in file_name:
    data = open(file, 'r').readlines()
    rows_records = []
    for i in data:
        result = ast.literal_eval(i)
        rows_records.append(result)
    final = pd.DataFrame(rows_records)

# %%
final

# %%
final.columns

# %%
final['ItemName'].unique()

# %%
df = final[['Mac','SessionMainMenu','AppName','LogId','Event','ItemId','RealTimePlaying']]

# %%
df

# %%
user_log = pd.read_csv('user_info.txt',delimiter='\t',low_memory=False)

# %%
user_log.rename(columns={'MAC':'Mac'},inplace=True)

# %%
user_log

# %%
user_log['Mac']=user_log['Mac'].str[4:]

# %%
user_log

# %%
df_info = user_log.merge(df,on = 'Mac')

# %%
df_info = df_info[['Mac','SessionMainMenu','AppName','LogId','Event','ItemId','RealTimePlaying']]

# %%
df_info

# %%
df_info = df_info.where(pd.notnull(df_info), None)
# %%
df_info['Date'] = df['SessionMainMenu'].str.extract(r'^[A-F0-9]+:(\d{4}:\d{2}:\d{2}):')

# %%
df_info['Date'] = pd.to_datetime(df_info['Date'], format='%Y:%m:%d')



# %%
def Import_to_database(user, password, database, port, data):
        cnx = mysql.connector.connect(user = user, password = password, database = database, port = port)
        cursor = cnx.cursor()
        sql = """INSERT INTO log_customer(
            Mac,
            SessionMainMenu,
            AppName,
            LogId,
            Event,
            ItemId,
            RealTimePlaying) VALUES (%s, %s, %s, %s, %s, %s, %s) """
        cursor.executemany(sql,data.values.tolist())
        cnx.commit()
        return print('Import Successfully')

# %%
Import_to_database(user = 'root', password = '', database = 'FPT', port = '3306', data = df_info)

# %%
p = 'B046FCA6B160:2016:02:03:10:10:36:945'

