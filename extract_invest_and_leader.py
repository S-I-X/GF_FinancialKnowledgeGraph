#!~/py3-env/bin/python3
# coding=utf-8

import pandas as pd
from tqdm import tqdm

df2 = pd.read_csv('../20180723/new_inv_company.csv', encoding='utf-8', sep='\t')
df3 = pd.read_csv('../20180723/new_inv_person.csv', encoding='utf-8', sep='\t')
df4 = pd.read_csv('../20180723/new_leader.csv', encoding='utf-8', sep='\t')
p_com = list(set(df2['ppripid'].values) | set(df2['pripid'].values))
df3['ok'] = 0
df4['ok'] = 0
for i in tqdm(range(len(p_com))):
    index = df3[df3['pripid'] == p_com[i]].index
    df3.loc[index, 'ok'] = 1
    index = df4[df4['pripid'] == p_com[i]].index
    df4.loc[index, 'ok'] = 1
df3[df3['ok'] == 1].iloc[:, :-2].to_csv('../20180727/simple_inv_person.csv', encoding='utf-8', index=0)
df4[df4['ok'] == 1].iloc[:, :-2].to_csv('../20180727/simple_leader.csv', encoding='utf-8', index=0)
