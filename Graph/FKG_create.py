#!~/py3-env/bin/python3
#coding=utf-8

from py2neo import Graph, Node, Relationship
import pandas as pd
import csv
import math
import time
from tqdm import tqdm

graph = Graph('http://10.35.85.32:7874', username='neo4j', password='123456', secure=False, bolt=False)  # 连接图数据库

def stock_process(stock=None):  # 股票代码格式化
    if stock[0] in ['0', '2', '3'] and len(stock) == 6:
        return stock+'.SZ'
    elif stock[0] in ['6', '9'] and len(stock) == 6:
        return stock+'.SH'
    else:
        return None

def create_company_all():  # 在图中创建所有的公司节点
    head = ['id','com_id','com_name','chi_sht','eng_name','eng_sht','natr_biz','natr_econ','reg_dt_first','reg_dt_chg',
            'found_dt','reg_cptl','curr_code','lgl_repr','gm','dire_secr','reg_addr']
    with open('../data/company/bas_com_info.csv', mode='r', encoding='utf8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter='\t')
        # print(file)
        for row in rows:
            node = Node('COMPANY')
            i = 0
            for value in row:
                try:
                    node[head[i]] = value
                    i += 1
                except:
                    continue
            graph.create(node)

def create_user_holding_stock():  # 在图中创建用户及其持股信息
    with open('../data/user/client_stock.csv', mode='r', encoding='utf8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=',')
        for row in rows:
            user_node = Node('USER')
            user_node['user_id'] = row[0]
            for stock in row[1].split('/'):
                stock = stock_process(stock.strip())
                if not stock:
                    continue
                try:
                    com_node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock)
                    if com_node:
                        rel = Relationship(user_node, 'U_Holding_S', com_node)
                        graph.merge(rel)
                except:
                    continue
    print('Over:create_user_holding_stock')

def create_user_focuson_stock():  # 在图中创建用户及其自选股信息
    with open('../data/user/client_selfstock.csv', mode='r', encoding='utf8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=',')
        for row in rows:
            user_node = Node('USER')
            user_node['user_id'] = row[0]
            for stock in row[1].split('/'):
                stock = stock_process(stock.strip())
                if not stock:
                    continue
                try:
                    com_node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock)
                    if com_node:
                        rel = Relationship(user_node, 'U_FocusOn_S', com_node)
                        graph.merge(rel)
                except:
                    continue
    print('Over:create_user_focuson_stock')

def create_user_browse_inf():  # 在图中创建用户及其浏览资讯信息
    with open('../data/user/client_time_news.csv', mode='r', encoding='utf8', newline='') as csvfile:
        rows = csv.reader(csvfile, delimiter=',')
        for row in rows:
            user_node = Node('USER')
            user_node['user_id'] = row[0]
            inf_node = Node('INFORMATION')
            inf_node['inf_id'] = row[2]
            rel = Relationship(user_node, 'U_Browse_INF', inf_node)
            rel['browse_dt'] = row[1]
            try:
                graph.merge(user_node | inf_node)
                graph.run(f"match (m:USER),(n:INFORMATION) where m.user_id='{row[0]}' and n.inf_id='{row[2]}' "
                          f"with m,n create (m)-[r:U_Browse_INF]->(n) set r.browse_dt='{row[1]}'")
            except:
                continue
    print('Over:create_user_browse_inf')

def merge_block():  # 在图中合并板块信息
    txtfile = open('../data/industry/concept.txt', mode='r', encoding='utf8', newline='')
    for row in txtfile.readlines():
        block_node = Node('BLOCK')
        block_node['block_name'] = row.strip()
        graph.merge(block_node)
    txtfile.close()
    print('Over:merge_block')

def create_inf_belongto_b():  # 在图中创建资讯及其所属板块信息
    csvfile = open('../data/information/news_concept.csv', mode='r', encoding='utf8', newline='')
    rows = csv.reader(csvfile)
    for i, row in enumerate(rows):
        if i == 0:
            continue
        inf_node = Node('INFORMATION')
        inf_node['inf_id'] = row[0]
        blocks = row[1].strip().split()
        for block in blocks:
            try:
                block_node = Node('BLOCK')
                block_node['block_name'] = block
                rel = Relationship(inf_node, 'INF_BelongTo_B', block_node)
                graph.merge(rel)
            except:
                continue
    csvfile.close()
    print('Over:create_inf_belongto_b')

def create_inf_belongto_com():  # 在图中创建资讯及其所属公司信息
    csvfile = open('../data/information/news_companies.csv', mode='r', encoding='utf8', newline='')
    rows = csv.reader(csvfile)
    for i, row in enumerate(rows):
        if i == 0:
            continue
        inf_node = Node('INFORMATION')
        inf_node['inf_id'] = row[0]
        coms = row[1].strip().split()
        for com in coms:
            try:
                com_node = Node('COMPANY')
                com_node['stock_code'] = com
                rel = Relationship(inf_node, 'INF_BelongTo_COM', com_node)
                graph.merge(rel)
            except:
                continue
    csvfile.close()
    print('Over:create_inf_belongto_com')

def create_inf_referto_ind():  # 在图中创建资讯所属行业信息（行业为资讯涉及公司所关联的行业）
    nodes = graph.data(
        f"match (n:INFORMATION)-[:INF_ReferTo_COM]->(:COMPANY)-[:COM_BelongTo_IND]->(m:INDUSTRY) return distinct n;")
    nodes = list(nodes)
    for i in tqdm(range(len(nodes)), desc='create_inf_referto_ind'):
        node = nodes[i]['n']
        inds = graph.data(f"match {node}-[:INF_ReferTo_COM]->(:COMPANY)-[:COM_BelongTo_IND]->(m:INDUSTRY) return m;")
        for ind in inds:
            ind = ind['m']
            rel = Relationship(node, 'INF_ReferTo_IND', ind)
            graph.create(rel)

def create_level3to4_industry():  # 在图中创建第三级行业标签下的第四级行业标签
    datas = graph.data(f"match (n:INDUSTRY) return distinct n.ind_name as ind_name;")
    inds = []
    for ind in datas:
        inds.append(ind['ind_name'])
    df1 = pd.read_csv('../Data/level3to4_industry.csv', encoding='utf-8', header=None, sep='\t')
    for i in tqdm(range(len(df1))):
        level3_ind = df1.iloc[i, 0]
        if level3_ind not in inds:
            print(df1.loc[i].values.tolist())
            continue
        level4_ind = df1.iloc[i, 1]
        node1 = Node('INDUSTRY')
        node1['ind_name'] = level3_ind
        node2 = Node('INDUSTRY4')
        node2['ind_name'] = level4_ind
        rel = Relationship(node1, 'IND_Level3to4', node2)
        graph.merge(rel)

if __name__ == '__main__':
    time1 = time.time()

    create_level3to4_industry()

    time2 = time.time()
    print('Time(s):', time2 - time1)
