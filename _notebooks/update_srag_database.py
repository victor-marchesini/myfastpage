#!/usr/bin/env python
# coding: utf-8
# %%

# %%

import os
import sqlite3 as sql
import pandas as pd

from srag_functions import *


# %%

# if not os.path.exists('data/opendatasus'):
os.makedirs('data/opendatasus',exist_ok=True)
    
print("Os arquivos serão salvos na pasta 'data/opendatasus' ")

# df_srag = get_srag_data(years=[2019,2020,2021],update=False,treat=True,save_local=True)

frames = []
for year in [2019,2020,2021]:
    df = get_srag_data(years=[year],update=True,treat=True,save_local=True)
    df['ano'] = year
    frames.append(df)
    
df_srag = pd.concat(frames)
del frames
del df


# %%

print('df_srag.shape:',df_srag.shape)


# %%


    
db_name = 'srag'
db_path = f'data/opendatasus/{db_name}.db'

conn = sql.connect(db_path)
df_srag.to_sql(db_name, conn, index=False, if_exists='replace')

print(f'data base saved as {db_name}.db')
del df_srag

# %%




