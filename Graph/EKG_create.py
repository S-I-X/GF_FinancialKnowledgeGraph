#!~/py3-env/bin/python3
# coding=utf-8

from py2neo import Graph, Node, Relationship
import pandas as pd
from tqdm import tqdm
from Graph.EKG_alg import Basic


graph = Graph('http://10.35.85.32:7473', username='neo4j', password='sysu', secure=False, bolt=False)  # 连接图数据库
rate_dict = {'人民币': 1.0000, '丹麦克朗': 1.0615, '德国马克': 4.0433, '日元': 0.0613, '欧元': 7.9081,
                          '加拿大元': 5.2089, '瑞士法郎': 6.8467, '阿富汗尼': 0.0941, '阿根廷比索': 0.2482,
                          '新加坡元': 4.9932, '美元': 6.8048, '新西兰元': 4.6100, '澳元': 5.0252, '法国法郎': 9.1995,
                          '英镑': 8.9186, '港元': 0.8660, '澳大利亚元': 5.0268, '韩元': 0.6071, '哥伦比亚比索': 0.0023}
exchange_rate = Basic.exchange_rate

# dff = pd.read_csv('../Data/processed/processed_new_inv_company.csv', encoding='utf-8', sep='\t').fillna('')
# full_com = list(set(dff['ppripid'].values) | set(dff['pripid'].values))

def create_all_company():  # 导入所有公司节点
    df1 = pd.read_csv('../Data/processed/unique_company.csv', encoding='utf-8', sep='\t').fillna('')
    columns = df1.columns.tolist()
    count = 0
    for i in tqdm(range(len(df1)), desc='create_all_company'):
        data = df1.loc[i]
        com_node = Node('COMPANY')
        for key in columns:
            com_node[key] = data[key]
        try:
            com_node['regcap_CNY'] = exchange_rate(float(data['regcap']), data['regcapcur_name'])
        except:
            continue
        try:
            graph.create(com_node)
            count += 1
        except:
            continue
    print('Finish create_all_company! All:{0}  Success:{1}'.format(i + 1, count))

def create_simple_company():  # 导入部分公司节点（暂时停用）
    df1 = pd.read_csv('../Data/processed/processed_simple_company.csv', encoding='utf-8', sep='\t').fillna('')
    columns = df1.columns.tolist()
    count = 0
    for i in tqdm(range(len(df1)), desc='create_simple_company'):
        data = df1.loc[i]
        com_node = Node('COMPANY')
        for key in columns:
            com_node[key] = data[key]
        try:
            com_node['regcap_CNY'] = exchange_rate(float(data['regcap']), data['regcapcur_name'])
        except:
            continue
        try:
            graph.create(com_node)
            count += 1
        except:
            continue
    print('Finish create_simple_company! All:{0}  Success:{1}'.format(i + 1, count))


def merge_all_company():  # 补全所有公司节点
    df1 = pd.read_csv('../Data/processed/unique_company.csv', encoding='utf-8', sep='\t').fillna('')
    columns = df1.columns.tolist()
    push_count = 0
    create_count = 0
    for i in tqdm(range(len(df1)), desc='merge_all_company'):
        if i < BREAKPOINT_COMPANY - 1:
            continue
        data = df1.loc[i]
        com_node = graph.find_one(label='COMPANY', property_key='pripid', property_value=data['pripid'])
        if com_node:
            if 'entname' not in com_node.keys():
                for key in columns[1:]:
                    com_node[key] = data[key]
                try:
                    com_node['regcap_CNY'] = exchange_rate(float(data['regcap']), data['regcapcur_name'])
                except:
                    continue
                try:
                    graph.push(com_node)
                    push_count += 1
                except:
                    continue
        else:
            com_node = Node('COMPANY')
            for key in columns:
                com_node[key] = data[key]
            try:
                graph.create(com_node)
                create_count += 1
            except:
                continue
    print('Finish merge_all_company! All:{0}  Push:{1}  Create:{2}'.format(i + 1, push_count, create_count))


def create_inv_company():  # 导入持股人为公司的持股关系
    df2 = pd.read_csv('../Data/processed/processed_new_inv_company.csv', encoding='utf-8', sep='\t').fillna('')
    columns = df2.columns.tolist()
    count = 0
    for i in tqdm(range(len(df2)), desc='create_inv_company'):
        if i < BREAKPOINT_INV_PERSON - 1:
            continue
        data = df2.loc[i]
        node1 = Node('COMPANY')
        node1['pripid'] = data[0]
        node2 = Node('COMPANY')
        node2['pripid'] = data[1]
        rel = Relationship(node1, 'Share', node2)
        rel['money_CNY'] = exchange_rate(data['subconam'], data['currency_name'])
        for key in columns[3:]:
            rel[key] = data[key]
        try:
            graph.merge(rel)
            count += 1
        except:
            continue
    print('Finish create_inv_company! All:{0}  Success:{1}'.format(i + 1, count))


def create_inv_person():  # 导入持股人为人的节点及持股关系
    df3 = pd.read_csv('../Data/processed/processed_new_inv_person.csv', encoding='utf-8', sep='\t').fillna('')
    columns = df3.columns.tolist()
    count = 0
    for i in tqdm(range(len(df3)), desc='create_inv_person'):
        if i < BREAKPOINT_INV_PERSON - 1:
            continue
        data = df3.loc[i]
        node1 = Node('PERSON')
        node1['name'] = data[1]
        node2 = Node('COMPANY')
        node2['pripid'] = data[0]
        rel = Relationship(node1, 'Share', node2)
        rel['money_CNY'] = exchange_rate(data['subconam'], data['currency_name'])
        for key in columns[2:]:
            rel[key] = data[key]
        try:
            graph.merge(rel)
            count += 1
        except:
            continue
    print('Finish create_inv_person! All:{0}  Success:{1}'.format(i + 1, count))


def create_leader():  # 导入董监高节点及关系
    df4 = pd.read_csv('../Data/processed/processed_new_leader.csv', encoding='utf-8', sep='\t').fillna('')
    columns = df4.columns.tolist()
    count = 0
    for i in tqdm(range(len(df4)), desc='create_leader'):
        if i < BREAKPOINT_LEADER - 1:
            continue
        data = df4.loc[i]
        node1 = Node('PERSON')
        node1['name'] = data[1]
        node2 = Node('COMPANY')
        node2['pripid'] = data[0]
        rel = Relationship(node1, 'WorkAt', node2)
        for key in columns[2:]:
            rel[key] = data[key]
        try:
            graph.merge(rel)
            count += 1
        except:
            continue
    print('Finish create_leader! All:{0}  Success:{1}'.format(i + 1, count))


BREAKPOINT_COMPANY = 1  # 补全公司的断点
BREAKPOINT_INV_COMPANY = 1  # 导入公司持股关系的断点
BREAKPOINT_INV_PERSON = 1  # 导入人持股关系的断点
BREAKPOINT_LEADER = 1  # 导入董监高关系的断点


if __name__ == '__main__':
    df1 = pd.read_csv('../Data/shareholders1.csv', encoding='utf-8', sep=',')
    for i in range(len(df1)):
        data = df1.iloc[i, :]
        entname = data['entname']
        regcap_CNY = data['regcap_CNY']
        holder_label, holder = (data['holder'].split('_'))[0], (data['holder'].split('_'))[1]
        money_CNY = float(data['money_CNY'])
        share_ratio = round(float((data['share_ratio'].split('%'))[0])/100, 6)
        FLAGS = str(data['flags'])
        com_node = graph.find_one(label='COMPANY', property_key='entname', property_value=entname)
        if not com_node:
            com_node = Node('COMPANY')
            com_node['entname'] = entname
            com_node['regcap_CNY'] = regcap_CNY
            com_node['FLAGS'] = FLAGS
            graph.create(com_node)
        else:
            com_node['regcap_CNY'] = regcap_CNY
            graph.push(com_node)
        if holder_label == 'COMPANY':
            holder_node = graph.find_one(label='COMPANY', property_key='entname', property_value=holder)
            if not holder_node:
                holder_node = Node('COMPANY')
                holder_node['entname'] = holder
                holder_node['FLAGS'] = FLAGS
                graph.create(holder_node)
        else:
            holder_node = graph.find_one(label='PERSON', property_key='name', property_value=holder)
            if not holder_node:
                holder_node = Node('PERSON')
                holder_node['name'] = holder
                holder_node['FLAGS'] = FLAGS
                graph.create(holder_node)
        rel = Relationship(holder_node, 'Share', com_node)
        rel['money_CNY'] = money_CNY
        rel['FLAGS'] = FLAGS
        graph.merge(rel)
        print(i+1, rel.start_node(), rel.relationships(), rel.end_node())
