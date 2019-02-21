#!~/py3-env/bin/python3
# coding=utf-8

import csv
import time

import numpy as np
import pandas as pd
from py2neo import Graph, walk
from tqdm import tqdm


class Basic(object):
    def __init__(self, http='http://10.35.85.32:7473', username='neo4j', password='sysu', secure=False, bolt=False):
        self.http = http
        self.username = username
        self.password = password
        self.secure = secure
        self.bolt = bolt
        self.graph = Graph(http, username=username, password=password, secure=False, bolt=False)  # 连接图数据库
        self.rate_dict = {'人民币': 1.0000, '丹麦克朗': 1.0615, '德国马克': 4.0433, '日元': 0.0613, '欧元': 7.9081,
                          '加拿大元': 5.2089, '瑞士法郎': 6.8467, '阿富汗尼': 0.0941, '阿根廷比索': 0.2482,
                          '新加坡元': 4.9932, '美元': 6.8048, '新西兰元': 4.6100, '澳元': 5.0252, '法国法郎': 9.1995,
                          '英镑': 8.9186, '港元': 0.8660, '澳大利亚元': 5.0268, '韩元': 0.6071, '哥伦比亚比索': 0.0023}

    def exchange_rate(self, fund=None, currency_name=None):  # 将不同货币按汇率换算成人民币
        if fund in ['', 'NULL', 'null', 'NaN']:
            return 0.0000
        if currency_name not in self.rate_dict.keys():
            rate = 1.0000
        else:
            rate = self.rate_dict[currency_name]
        return float(fund) * rate


class InitCompute(Basic):
    def compute_all_regcap(self):  # 初始化计算所有不包含注册资本的公司节点的注册资本
        datas = self.graph.data(f"match (n:COMPANY)<-[:Share]-() where not exists(n.regcap) or n.regcap='' "
                                f"or not exists(n.regcap_CNY) or n.regcap_CNY='' return n;")
        for i in tqdm(range(len(datas)), desc='compute_all_regcap'):
            node = datas[i]['n']
            rels = self.graph.data(f"match {node}<-[r:Share]-() return r")
            regcap = 0.0000
            for rel in rels:
                rel = rel['r']
                regcap += float(rel['money_CNY'])
            node['regcap_CNY'] = regcap
            self.graph.push(node)

    def compute_one_shareratio(self, node=None, pripid=None, entname=None):  # 计算一个节点所有的被持股比例
        rels = []
        if node:
            rels = self.graph.data(f"match {node}<-[r:Share]-() return r")
        if pripid:
            rels = self.graph.data(f"match (n:COMPANY)<-[r:Share]-() where n.pripid='{pripid}' return r")
        if entname:
            rels = self.graph.data(f"match (n:COMPANY)<-[r:Share]-() where n.entname='{entname}' return r")
        count = 0
        for rel in rels:
            rel = rel['r']
            count += self.exchange_rate(rel['subconam'], rel['currency_name'])
        for rel in rels:
            rel = rel['r']
            if count == 0.0000:
                ratio = 0.0000
            else:
                ratio = round(float(rel['subconam']) / count, 6)
            rel['share_ratio'] = ratio
            self.graph.push(rel)

    def init_compute_all_shareratio(self):  # 初始化计算所有公司的被持股比例
        entnames = self.graph.data(f"match (n:COMPANY)<-[:Share]-() return n.entname as entname;")
        for i in tqdm(range(len(entnames)), desc='init_compute_all_shareratio'):
            entname = entnames[i]['entname']
            self.compute_one_shareratio(entname=entname)


class Search(Basic):
    def search_one_actual_controller_paths(self, pripid=None, entname=None, min_level=1,
                                           max_level=10):  # 在图中计算某一个公司的最终持股人列表、持股比例及其路径
        if pripid and entname:
            com_paths = self.graph.data(
                f"match (n:COMPANY) where n.pripid='{pripid}' and n.entname='{entname}' call apoc.path.expand"
                f"(n,'<Share','>COMPANY',{min_level},{max_level}) yield path return path")
            per_paths = self.graph.data(
                f"match (n:COMPANY) where n.pripid='{pripid}' and n.entname='{entname}' call apoc.path.expand"
                f"(n,'<Share','>PERSON',{min_level},{max_level}) yield path return path")
        elif pripid:
            com_paths = self.graph.data(
                f"match (n:COMPANY) where n.pripid='{pripid}' call apoc.path.expand"
                f"(n,'<Share','>COMPANY',{min_level},{max_level}) yield path return path")
            per_paths = self.graph.data(
                f"match (n:COMPANY) where n.pripid='{pripid}' call apoc.path.expand"
                f"(n,'<Share','>PERSON',{min_level},{max_level}) yield path return path")
        elif entname:
            com_paths = self.graph.data(
                f"match (n:COMPANY) where n.entname='{entname}' call apoc.path.expand"
                f"(n,'<Share','>COMPANY',{min_level},{max_level}) yield path return path")
            per_paths = self.graph.data(
                f"match (n:COMPANY) where n.entname='{entname}' call apoc.path.expand"
                f"(n,'<Share','>PERSON',{min_level},{max_level}) yield path return path")
        else:
            return False
        paths = com_paths + per_paths
        if paths:
            endnodes = []
            ratios = []
            max_length = 0
            for path in paths:
                path = path['path']
                endnodes.append(path.end_node())
                path_nodes = path.nodes()
                path_rels = path.relationships()
                ratio = 1.0
                if len(path_rels) > max_length:
                    max_length = len(path_rels)
                for i, rel in enumerate(path_rels):
                    # ratio *= float(rel['share_ratio'])  # 采用初始化持股比例的方案计算实际控制人
                    if 'regcap_CNY' in path_nodes[i].keys() and 'money_CNY' in path_rels[i].keys():
                        if float(path_nodes[i]['regcap_CNY']) > 0:
                            ratio *= float(rel['money_CNY']) / float(path_nodes[i]['regcap_CNY'])  # 采用认缴金额/注册资本的持股比例方案计算实际控制人
                ratios.append(ratio)
            rank = np.argsort(ratios)
            repeat_index = []
            for i in range(len(paths)):
                for j in range(len(paths)):
                    if i == j:
                        continue
                    node_set1 = list(paths[i]['path'].nodes())
                    node_set2 = list(paths[j]['path'].nodes())
                    rel_set1 = list(paths[i]['path'].relationships())
                    rel_set2 = list(paths[j]['path'].relationships())
                    if len(node_set1) <= len(node_set2) and node_set1 == node_set2[
                                                                         :len(node_set1)] and rel_set1 == rel_set2[:len(
                            rel_set1)]:
                        repeat_index.append(i)
                        break
            sorted_ratios, sorted_nodes, sorted_paths = [], [], []
            for index in rank:
                if index in repeat_index:
                    continue
                sorted_ratios.append(ratios[index])
                sorted_nodes.append(endnodes[index])
                sorted_paths.append(paths[index]['path'])
            return sorted_ratios[::-1], sorted_nodes[::-1], sorted_paths[::-1], max_length
        else:
            return None


class Print(object):
    def __init__(self, datas=None, limit=10, min_ratio=0.0, particular_nodes=False, particular_paths=False):
        self.datas = datas
        self.limit = limit
        self.min_ratio = min_ratio
        self.particular_nodes = particular_nodes
        self.particular_paths = particular_paths

    def print_actual_controller_paths(self, datas=None, limit=10, min_ratio=0.0, particular_nodes=False,
                                      particular_paths=False):
        if not datas:
            print('Not found!')
            return False
        print(f"Paths Num:{len(datas[0])}\t\tMax Length:{datas[3]}\t\tRatio Sum:{sum(datas[0])}")
        for i in range(len(datas[0])):
            if i >= limit or datas[0][i] < min_ratio:
                break
            node_names = []
            for node in datas[2][i].nodes():
                if 'PERSON' in node.labels():
                    node_name = 'PERSON_' + node['name']
                elif 'COMPANY' in node.labels():
                    if 'entname' in node.keys():
                        node_name = 'COMPANY_' + node['entname']
                    else:
                        node_name = 'Pripid_' + node['pripid']
                else:
                    node_name = 'Null'
                node_names.append(node_name)
            print_path = node_names
            if particular_nodes:
                print_path = datas[2][i].nodes()
            if particular_paths:
                print_path = list(walk(datas[2][i]))
            if 'PERSON' in datas[1][i].labels():
                leaf_node = 'PERSON_' + datas[1][i]['name']
            elif 'entname' in datas[1][i].keys():
                leaf_node = 'COMPANY_' + datas[1][i]['entname']
            elif 'pripid' in datas[1][i].keys():
                leaf_node = 'Pripid_' + datas[1][i]['pripid']
            else:
                leaf_node = 'Null'
            print(f"{i+1}\t\tRatio:{datas[0][i]}\t\tLeaf Node:{leaf_node}\t\tPath:{print_path}")
        return True

    def print_actual_controllers(self, datas=None, printing=True, limit=10, min_ratio=0.0):
        if not datas:
            if printing:
                print('Not found!')
            return None
        if printing:
            print(f"Paths Num:{len(datas[0])}\t\tMax Length:{datas[3]}\t\tRatio Sum:{sum(datas[0])}")
        leaf_node_dict = {}
        for i in range(len(datas[0])):
            if 'PERSON' in datas[1][i].labels():
                leaf_node = 'PERSON_' + datas[1][i]['name']
            elif 'entname' in datas[1][i].keys():
                leaf_node = 'COMPANY_' + datas[1][i]['entname']
            elif 'pripid' in datas[1][i].keys():
                leaf_node = 'Pripid_' + datas[1][i]['pripid']
            else:
                leaf_node = 'Null'
            if leaf_node not in leaf_node_dict.keys():
                leaf_node_dict[leaf_node] = [1, datas[0][i]]
            else:
                leaf_node_dict[leaf_node][0] += 1
                leaf_node_dict[leaf_node][1] += datas[0][i]
        sorted_leaf_nodes = sorted(leaf_node_dict, key=lambda x: leaf_node_dict[x][1], reverse=True)
        results = []
        for i, node in enumerate(sorted_leaf_nodes):
            if i >= limit or leaf_node_dict[node][1] < min_ratio:
                continue
            results.append([node, leaf_node_dict[node][0], leaf_node_dict[node][1]])
            if printing:
                print(f"{i+1}\t\tLeaf Node:{node}\t\tPaths Num:{leaf_node_dict[node][0]}\t\tTotal Ratio:{leaf_node_dict[node][1]}")
        return results

def verify_actual_controller():
    s = Search()
    p = Print()
    df1 = pd.read_csv('../Data/Verify/actual_controller.csv', encoding='utf-8', sep='\t').fillna('')
    df1['graph_actual_controller'] = ''
    df1['graph_ratio'] = 0.0
    df1['existing'] = 0
    df1['verified'] = 0
    existing_count = 0
    verified_count = 0
    for i in tqdm(range(len(df1)), desc='verify_actual_controller'):
        try:
            reg_num_biz = df1.iloc[i, 0]
            if reg_num_biz != '':
                com = s.graph.find_one(label='COMPANY', property_key='credit_code', property_value=reg_num_biz)
                if com:
                    df1.iloc[i, 9] = 1
                    existing_count += 1
                    datas = s.search_one_actual_controller_paths(entname=com['entname'], min_level=1, max_level=10)
                    results = p.print_actual_controllers(datas, printing=False)
                    if results:
                        node_flag = results[0][0]
                        df1.iloc[i, 7] = node_flag
                        df1.iloc[i, 8] = results[0][2]*100
                        node_name = (node_flag.split('_'))[1]
                        if node_name == df1.iloc[i, 4]:
                            df1.iloc[i, 10] = 1
                            verified_count += 1
                            with open('../Data/Verify/verified_records.csv', mode='a', encoding='utf-8',
                                      newline='') as csvfile:
                                writer = csv.writer(csvfile, delimiter='\t')
                                writer.writerow(df1.iloc[i, :-2].values.tolist())
            with open('../Data/Verify/all_records.csv', mode='a', encoding='utf-8', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter='\t')
                writer.writerow(df1.iloc[i, :].values.tolist())
        except:
            print(f"Error:{i}")
            continue
    df2 = df1.sort_values(by=['verified', 'existing', 'graph_ratio', 'reg_num_biz'], ascending=[0, 0, 0, 1])
    df2.to_csv('../Data/Verify/processed_all_records.csv', index=0, encoding='utf-8', sep='\t')
    print(f"All Num:{len(df1)}\tExisting Num:{existing_count}\tVerified Num:{verified_count}")

if __name__ == '__main__':
    time1 = time.time()

    verify_actual_controller()
    # p.print_actual_controller_paths(s.search_one_actual_controller_paths(entname='广发证券股份有限公司'), limit=10, min_ratio=0.05)

    time2 = time.time()
    print('Time(s):', time2 - time1)
