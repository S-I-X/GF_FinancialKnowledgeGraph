#!~/py3-env/bin/python3
# coding=utf-8

import pandas as pd

df1 = pd.read_csv('./tyzx_share.csv', encoding='utf-8', sep='\t').fillna('')
df2 = df1.drop_duplicates(subset=['com_id', 'sh_name'], keep='first', inplace=False)
df2.to_csv('./new_tyzx_share.csv', encoding='utf-8', sep='\t', index=0)