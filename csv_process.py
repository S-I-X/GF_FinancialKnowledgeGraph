#!~/py3-env/bin/python3
#coding=utf-8

import csv

def process_leader():
    with open('leader.csv', mode='r', encoding='utf-8', newline='') as csvfile, open('new_leader.csv', mode='w', encoding='utf-8', newline='') as csvfile2:
        rows = csv.reader(csvfile, delimiter='\t')
        writer = csv.writer(csvfile2, delimiter='\t')
        col1, col2, col3 = '', '', ''
        for i, row in enumerate(rows):
            if i == 0:
                columns = row
                writer.writerow(row)
                continue
            if len(row) != len(columns):
                continue
            if row[0] == col1 and row[1] == col2 and row[3] == col3:
                continue
            else:
                col1, col2, col3 = row[0], row[1], row[3]
                writer.writerow(row)

def process_inv_company():
    with open('inv_company.csv', mode='r', encoding='utf-8', newline='') as csvfile, open('new_inv_company.csv', mode='w', encoding='utf-8', newline='') as csvfile2:
        rows = csv.reader(csvfile, delimiter='\t')
        writer = csv.writer(csvfile2, delimiter='\t')
        col1, col2 = '', ''
        for i, row in enumerate(rows):
            if i == 0:
                columns = row
                writer.writerow(row)
                continue
            if len(row) != len(columns):
                continue
            if row[5] in ['', 'null', 'NULL']:
                continue
            if float(row[5]) == 0:
                continue
            if row[0] == col1 and row[1] == col2:
                continue
            else:
                col1, col2 = row[0], row[1]
                writer.writerow(row)

def process_inv_person():
    with open('inv_person.csv', mode='r', encoding='utf-8', newline='') as csvfile, open('new_inv_person.csv', mode='w', encoding='utf-8', newline='') as csvfile2:
        rows = csv.reader(csvfile, delimiter='\t')
        writer = csv.writer(csvfile2, delimiter='\t')
        col1, col2 = '', ''
        for i, row in enumerate(rows):
            if i == 0:
                columns = row
                writer.writerow(row)
                continue
            if len(row) != len(columns):
                continue
            if row[4] in ['', 'null', 'NULL']:
                continue
            if float(row[4]) == 0:
                continue
            if row[0] == col1 and row[1] == col2:
                continue
            else:
                col1, col2 = row[0], row[1]
                writer.writerow(row)

if __name__ == '__main__':
    process_inv_person()
