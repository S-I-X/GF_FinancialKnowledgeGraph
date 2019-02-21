#!~/py3-env/bin/python3
# coding=utf-8
import csv
import json

FILE_PATH = '../data/'
SOURCE_FILE = FILE_PATH + '394_user_93911_information_with_browse_20180912.csv'

USER_JSON = FILE_PATH + 'default_users.json'
INF_JSON = FILE_PATH + 'default_informations.json'
BROWSE_JSON = FILE_PATH + 'default_users_browse.json'

csvfile1 = open(FILE_PATH + SOURCE_FILE, mode='r', encoding='utf-8', newline='')
rows = csv.reader(csvfile1, delimiter='\t')

user_json = {}
inf_json = {}
browse_json = {}
for i, row in enumerate(rows):
    if i == 0:
        cols = row
        continue
    if row[10] in ['U_FocusOn_S', 'U_Holding_S']:
        user = 'USER_' + row[1]
        value = row[4].strip().replace(' ', '-')
        if user not in user_json.keys():
            user_json[user] = [value]
        else:
            user_json[user] = user_json[user] + [value]

    if row[10] in ['INF_ReferTo_COM']:
        inf = 'INFORMATION_' + row[2]
        value = row[4].strip().replace(' ', '-')
        if inf not in inf_json.keys():
            inf_json[inf] = [value]
        else:
            inf_json[inf] = inf_json[inf] + [value]
    if row[10] in ['INF_ReferTo_B']:
        inf = 'INFORMATION_' + row[2]
        value = 'BLOCK_' + row[6]
        if inf not in inf_json.keys():
            inf_json[inf] = [value]
        else:
            inf_json[inf] = inf_json[inf] + [value]
    if row[10] in ['INF_ReferTo_IND']:
        inf = 'INFORMATION_' + row[2]
        value = 'INDUSTRY_' + row[5]
        if inf not in inf_json.keys():
            inf_json[inf] = [value]
        else:
            inf_json[inf] = inf_json[inf] + [value]

    if row[10] in ['U_Browse_INF']:
        user = 'USER_' + row[1]
        value = 'INFORMATION_' + row[7]
        if user not in browse_json.keys():
            browse_json[user] = [value]
        else:
            browse_json[user] = browse_json[user] + [value]

with open(USER_JSON, mode='w', encoding='utf-8', newline='') as file1:
    json.dump(user_json, file1)
with open(INF_JSON, mode='w', encoding='utf-8', newline='') as file1:
    json.dump(inf_json, file1)
with open(BROWSE_JSON, mode='w', encoding='utf-8', newline='') as file1:
    json.dump(browse_json, file1)
