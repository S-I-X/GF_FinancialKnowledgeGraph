#!~/py3-env/bin/python3
#coding=utf-8

from py2neo import Graph, Node, Relationship
import pandas as pd
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

def update_user_holding_stock():  # 在图中更新用户及其持股信息
    df1 = pd.read_csv('../data/0823_graph_data/users_stock_0823.csv', encoding='utf-8', sep='\t', header=None, dtype='str')
    for i in tqdm(range(df1.shape[0]), desc='update_user_holding_stock'):
        user_node = Node('USER')
        user_node['user_id'] = df1.iloc[i, 0]
        for stock in df1.iloc[i, 1].strip().split():
            stock = stock_process(stock)
            if not stock:
                continue
            try:
                com_node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock)
                if com_node:
                    rel = Relationship(user_node, 'U_Holding_S', com_node)
                    graph.merge(rel)
            except:
                continue

def update_user_focuson_stock():  # 在图中更新用户及其自选股信息
    df1 = pd.read_csv('../data/0823_graph_data/users_self_0823.csv', encoding='utf-8', sep='\t', header=None, dtype='str')
    for i in tqdm(range(df1.shape[0]), desc='update_user_focuson_stock'):
        user_node = Node('USER')
        user_node['user_id'] = df1.iloc[i, 0]
        for stock in df1.iloc[i, 1].strip().split():
            stock = stock_process(stock)
            if not stock:
                continue
            try:
                com_node = graph.find_one(label='COMPANY', property_key='stock_code', property_value=stock)
                if com_node:
                    rel = Relationship(user_node, 'U_FocusOn_S', com_node)
                    graph.merge(rel)
            except:
                continue

def update_user_browse_inf():  # 在图中更新用户及其浏览资讯信息
    df1 = pd.read_csv('../data/0823_graph_data/users_news_0823.csv', encoding='utf-8', sep='\t', header=None, dtype='str')
    for i in tqdm(range(df1.shape[0]), desc='update_user_browse_inf'):
        user_node = Node('USER')
        user_node['user_id'] = df1.iloc[i, 0]
        for dd in df1.iloc[i, 1].strip().split():
            inf_node = Node('INFORMATION')
            inf_node['inf_id'] = dd
            rel = Relationship(user_node, 'U_Browse_INF', inf_node)
            try:
                graph.merge(rel)
            except:
                continue

def update_inf_referto_b():  # 在图中更新资讯及其所属板块信息
    df1 = pd.read_csv('../data/0823_graph_data/news_concept_0813.csv', encoding='utf-8', sep=',', names=[0, 1], dtype='str')
    for i in tqdm(range(df1.shape[0]), desc='update_inf_referto_b'):
        inf_node = Node('INFORMATION')
        inf_node['inf_id'] = df1.iloc[i, 0]
        for block in df1.iloc[i, 1].strip().split():
            block_node = Node('BLOCK')
            block_node['block_name'] = block
            rel = Relationship(inf_node, 'INF_ReferTo_B', block_node)
            try:
                graph.merge(rel)
            except:
                continue

def update_inf_referto_com():  # 在图中更新资讯及其所属公司信息
    df1 = pd.read_csv('../data/0823_graph_data/news_stock_0821.csv', encoding='utf-8', sep='\t', names=[0, 1], dtype='str')
    for i in tqdm(range(df1.shape[0]), desc='update_inf_referto_com'):
        inf_node = Node('INFORMATION')
        inf_node['inf_id'] = df1.iloc[i, 0]
        for com in df1.iloc[i, 1].strip().split():
            com_node = Node('COMPANY')
            com_node['stock_code'] = stock_process(com)
            rel = Relationship(inf_node, 'INF_ReferTo_COM', com_node)
            try:
                graph.merge(rel)
            except:
                continue

if __name__ == '__main__':
    time1 = time.time()

    update_user_browse_inf()

    time2 = time.time()
    print('Time(s):', time2 - time1)
