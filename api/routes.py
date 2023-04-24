
from flask import request
from flask import Flask
#from flask_restx import Api

import pandas
import json
import sys
from .config import BaseConfig
import requests


#rest_api = Api(version="1.0", title="TJ API")
rest_api = Flask(__name__)

@rest_api.route('/api/data/add', methods=['PUT'])
def put_data1():
    data = request.get_json()
    with open('db.json', 'r+') as file:
        db_data = json.loads(file.read())
        file.seek(0)

        if len(db_data['deals_pull']) == 0:
            db_data['deals_pull'] = [data]
        else:
            db_data['deals_pull'][0]['parameters'] = data['parameters']
            db_data['deals_pull'][0]['deals'] = {key: data['deals'].get(key, db_data['deals_pull'][0]['deals'][key]) for key in db_data['deals_pull'][0]['deals']}
            db_data['deals_pull'][0]['deals'] = dict(db_data['deals_pull'][0]['deals'], **data['deals'])

        file.write(json.dumps(db_data, indent=4))
        file.truncate()
    return {
            "success": True,
           }, 200

@rest_api.route('/api/data/get')
def get_data1():
    with open('db.json', 'r') as file:
        data = json.loads(file.read())['deals_pull']
        if len(data) == 0:
            data = {
                    "parameters": [],
                    "deals": {}
                }
        else:
            data = data[0]
    return data, 200


@rest_api.route('/api/rec/get')
def get_recomendations():
    with open('db.json', 'r') as file:
        cont = json.loads(file.read())
    if 'deals_pull' not in cont:
        return {}
        
    my_df = pandas.DataFrame.from_dict(cont['deals_pull'][0]['deals'], orient='index').sort_values(by=['start-date', 'start-time'])
    my_df = my_df.reset_index()

    history = {}

    for index, row in my_df.iterrows():
        if not pandas.isnull(row['amount']):
            if row['deal-type'] == 'Покупка':
                if row['asset-code'] in history:
                    history[row['asset-code']].append((history[row['asset-code']][-1][0] + float(row['amount']), row['index']))
                else:
                    history[row['asset-code']] = [(float(row['amount']), row['index'])]
            else:
                if row['asset-code'] in history:
                    history[row['asset-code']].append((history[row['asset-code']][-1][0] - float(row['amount']), row['index']))
                else:
                    history[row['asset-code']] = [(-float(row['amount']), row['index'])]
                    
    history_with_groups = {}

    for currency in history:
        history_with_groups[currency]= []
        
        current_group = []
        for operation in history[currency]:
            if operation[0] != 0.0:
                current_group.append(operation)
            else:
                current_group.append(operation)
                history_with_groups[currency].append(current_group)
                current_group = []
        if len(current_group) > 0:
            history_with_groups[currency].append(current_group)
            
    def get_row_by_id(x):
        z = my_df[my_df['index'] == x]
        idx = z.index[0]
        r = z.to_dict()
        return {k: r[k][idx] for k in r}

    def get_rub_price(row):
        if row['price-currency'] == 'RUB':
            return float(row['unit-price'])
        elif row['price-currency'] == 'USD':
            return float(row['unit-price']) * 80.0
        else:
            return 0.
        
    new_group_value = []
    prof_per_curr = {}
    history_with_groups_and_profit = {}
    for key in history_with_groups:
        profit_per_group = []
        history_with_groups_and_profit[key] = []
        for idx, val in enumerate(history_with_groups[key]):
            group_prof = 0.
            for op in val:
                cur_op = get_row_by_id(op[1])
                if cur_op['deal-type'] == 'Покупка':
                    pro = - float(cur_op['amount']) * get_rub_price(cur_op)
                else:
                    pro = float(cur_op['amount']) * get_rub_price(cur_op)
                    group_prof += pro
            profit_per_group.append(group_prof)
        for idx, val in enumerate(history_with_groups[key]):
            history_with_groups_and_profit[key].append((val, profit_per_group[idx]))
            
    def most_common(lst):
        return max(set(lst), key=lst.count)

    new_pull = []

    for key in history_with_groups_and_profit:
        for group in history_with_groups_and_profit[key]:
            actual_group = group[0]
            group_list = get_row_by_id(actual_group[0][1])
            for ag in actual_group:
                for op_key in get_row_by_id(ag[1]):
                    if type(group_list[op_key]) is list:
                        group_list[op_key].append(get_row_by_id(ag[1])[op_key])
                    else:
                        group_list[op_key] = [get_row_by_id(ag[1])[op_key]]
            for gk in group_list:
                group_list[gk] = most_common(group_list[gk])
            new_object = {}
            if 'market' in group_list:
                new_object['market'] = group_list['market']
            if 'trading-mode' in group_list:
                new_object['trading-mode'] = group_list['trading-mode']
            if 'asset-code' in group_list:
                new_object['asset-code'] = group_list['asset-code']
            if 'broker' in group_list:
                new_object['broker'] = group_list['broker']
            new_object['analytics'] = group[1]
            new_pull.append(new_object)
            
    pull_df = pandas.DataFrame(new_pull)
    
    count_plus = lambda x: x[x >= 0.0].count()
    count_plus.__name__ ='count_plus'
    count_minus = lambda x: x[x < 0.0].count()
    count_minus.__name__ ='count_minus'
    response = pull_df.groupby(list(pull_df.columns[:-1])).agg({
        'analytics': ['sum', count_plus, count_minus]
    })
    
    response.columns = ['_'.join(col).strip() for col in response.columns.values]
    
    return response.reset_index().to_dict(orient="split")