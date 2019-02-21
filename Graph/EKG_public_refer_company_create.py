#!~/py3-env/bin/python3
# coding=utf-8

import csv

from py2neo import Graph, Node, Relationship
from tqdm import tqdm

graph = Graph('http://10.35.85.32:7473', username='neo4j', password='sysu', secure=False, bolt=False)  # 连接图数据库


def create_public_refer_company():  # 导入公司节点
    csvfile = open('../20180726/public_refer_company.csv', mode='r', encoding='utf-8')
    datas = csv.reader(csvfile, delimiter='\t')
    rows = list(datas)
    csvfile.close()
    count = 0
    for i in tqdm(range(len(rows))):
        row = rows[i]
        if i == 0:
            columns = row
            continue
        com_node = Node('COMPANY')
        for j, col in enumerate(columns):
            com_node[col] = row[j]
        try:
            graph.create(com_node)
            count += 1
        except:
            continue
    print('Finish create_public_refer_company! All:{0}  Success:{1}'.format(i, count))


def create_public_refer_inv_company():  # 导入持股人为公司的持股关系
    csvfile = open('../20180726/public_refer_inv_company.csv', mode='r', encoding='utf-8')
    datdas = csv.reader(csvfile, delimiter='\t')
    rows = list(datdas)
    csvfile.close()
    count = 0
    for i in tqdm(range(len(rows))):
        row = rows[i]
        if i == 0:
            columns = row
            continue
        node1 = Node('COMPANY')
        node1['pripid'] = row[0]
        node2 = Node('COMPANY')
        node2['pripid'] = row[1]
        rel = Relationship(node1, 'Share', node2)
        for j, col in enumerate(columns):
            if j < 3:
                continue
            rel[col] = row[j]
        try:
            graph.merge(rel)
            count += 1
        except:
            continue
    print('Finish create_public_refer_inv_company! All:{0}  Success:{1}'.format(i, count))


def create_public_refer_inv_person():  # 导入持股人为人的节点及持股关系
    csvfile = open('../20180726/public_refer_inv_person.csv', mode='r', encoding='utf-8')
    inv_coms = csv.reader(csvfile, delimiter='\t')
    rows = list(inv_coms)
    csvfile.close()
    count = 0
    for i in tqdm(range(len(rows))):
        row = rows[i]
        if i == 0:
            columns = row
            continue
        com_node = Node('COMPANY')
        com_node['pripid'] = row[0]
        person_node = Node('PERSON')
        person_node['name'] = row[1]
        rel = Relationship(com_node, 'Share', person_node)
        for j, col in enumerate(columns):
            if j < 2:
                continue
            rel[col] = row[j]
        try:
            graph.merge(rel)
            count += 1
        except:
            continue
    print('Finish create_public_refer_inv_person! All:{0}  Success:{1}'.format(i, count))


def create_public_refer_leader():  # 导入董监高节点及关系
    csvfile = open('../20180726/public_refer_leader.csv', mode='r', encoding='utf-8')
    datas = csv.reader(csvfile, delimiter='\t')
    rows = list(datas)
    csvfile.close()
    count = 0
    for i in tqdm(range(len(rows))):
        row = rows[i]
        if i == 0:
            columns = row
            continue
        com_node = Node('COMPANY')
        com_node['pripid'] = row[0]
        leader_node = Node('PERSON')
        leader_node['name'] = row[1]
        rel = Relationship(leader_node, 'WorkAt', com_node)
        for j, col in enumerate(columns):
            if j < 2:
                continue
            rel[col] = row[j]
        try:
            graph.merge(rel)
            count += 1
        except:
            continue
    print('Finish create_public_refer_leader! All:{0}  Success:{1}'.format(i, count))


if __name__ == '__main__':
    create_public_refer_company()
    create_public_refer_inv_company()
    create_public_refer_inv_person()
    create_public_refer_leader()
