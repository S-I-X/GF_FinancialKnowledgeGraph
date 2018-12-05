#!~/py3-env/bin/python3
# coding=utf-8
import csv
import pandas as pd

FILE_PATH = '../data/'
SOURCE_FILE = FILE_PATH + '394_user_93911_information_with_browse_20180912.csv'
OBJECT_FILE = FILE_PATH + 'edge_394_user_93911_information_without_browse_20180912.csv'
WITH_BROWSE = 1


def str_process(lis=None):
    for i, l in enumerate(lis):
        lis[i] = lis[i].strip().replace(' ', '-')
    return lis


csvfile1 = open(SOURCE_FILE, mode='r', encoding='utf-8', newline='')
csvfile2 = open(OBJECT_FILE, mode='w', encoding='utf-8', newline='')
rows = csv.reader(csvfile1, delimiter='\t')
writer = csv.writer(csvfile2, delimiter='\t')
for i, row in enumerate(rows):
    if i == 0:
        cols = row
        continue
    if row[10] in ['COM_Invest_COM', 'COM_Output_COM']:
        writer.writerow(str_process([row[0], row[4]]))
    if row[10] in ['COM_BelongTo_IND']:
        writer.writerow(str_process([row[0], 'INDUSTRY_' + row[5]]))
    if row[10] in ['COM_BelongTo_B']:
        writer.writerow(str_process([row[0], 'BLOCK_' + row[6]]))
    if row[10] in ['U_FocusOn_S', 'U_Holding_S']:
        writer.writerow(str_process(['USER_' + row[1], row[4]]))
    if row[10] in ['U_Browse_INF'] and WITH_BROWSE == 1:
        writer.writerow(str_process(['USER_' + row[1], 'INFORMATION_' + row[7]]))
    if row[10] in ['INF_ReferTo_COM']:
        writer.writerow(str_process(['INFORMATION_' + row[2], row[4]]))
    if row[10] in ['INF_ReferTo_B']:
        writer.writerow(str_process(['INFORMATION_' + row[2], 'BLOCK_' + row[6]]))
    if row[10] in ['INF_ReferTo_IND']:
        writer.writerow(str_process(['INFORMATION_' + row[2], 'INDUSTRY_' + row[5]]))
    if row[10] in ['IND_Level3to4']:
        writer.writerow(str_process(['INDUSTRY_' + row[3], 'INDUSTRY4_' + row[5]]))
csvfile2.close()
csvfile1.close()
df1 = pd.read_csv(FILE_PATH + OBJECT_FILE, header=None, encoding='utf-8', sep='\t')
df2 = df1.sort_values(by=[0])
df2.to_csv(FILE_PATH + OBJECT_FILE, header=None, index=0, encoding='utf-8', sep='\t')
