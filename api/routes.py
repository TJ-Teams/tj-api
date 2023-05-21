
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

@rest_api.route('/api/data/set', methods=['PUT'])
def put_data1():
    data = request.get_json()
    with open('db.json', 'r+') as file:
        db_data = json.loads(file.read())
        file.seek(0)

        #if len(db_data['deals_pull']) == 0:
        #    db_data['deals_pull'] = [data]
        #else:
            #db_data['deals_pull'][0]['parameters'] = data['parameters']
            #db_data['deals_pull'][0]['deals'] = {key: data['deals'].get(key, db_data['deals_pull'][0]['deals'][key]) for key in db_data['deals_pull'][0]['deals']}
            #db_data['deals_pull'][0]['deals'] = dict(db_data['deals_pull'][0]['deals'], **data['deals'])
            
        db_data['deals_pull'] = [data]

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


COLUMN_MAPPING = {
    'asset-code': ['name'],
    'market': ['marketplace']
}

def get_dfc(pd_df, col):
    if col in COLUMN_MAPPING:
        for c in COLUMN_MAPPING[col]:
            if c in pd_df.columns:
                return pd_df[c]
    return pd_df[col]

def get_dfcs(pd_df, col):
    if col in COLUMN_MAPPING:
        for c in COLUMN_MAPPING[col]:
            if c in pd_df.columns:
                return c
    return col

def get_rowc(row, col):
    if col in COLUMN_MAPPING:
        for c in COLUMN_MAPPING[col]:
            if c in row:
                return c
    return col


def get_sf_columns(obj):
    result = []
    just_keys = [x['key'] for x in obj]
    if 'date' in just_keys:
        result.append('date')
    if 'start-time' in just_keys:
        result.append('start-time')
    return result

def get_recomendations(startDate=None, endDate=None, groupKeys=None):
    with open('db.json', 'r') as file:
        cont = json.loads(file.read())
    if 'deals_pull' not in cont:
        return {}
        
    my_df = pandas.DataFrame.from_dict(cont['deals_pull'][0]['deals'], orient='index')
    my_df = my_df.reset_index()
    
    if 'date' in my_df.columns:
        my_df['date'] = pandas.to_datetime(my_df['date'], format='%Y-%m-%d')
        if startDate is not None:
            my_df = my_df[my_df['date'] >= pandas.to_datetime(startDate, format='%Y-%m-%d')]
        if endDate is not None:
            my_df = my_df[my_df['date'] <= pandas.to_datetime(endDate, format='%Y-%m-%d')]
    
    my_df = my_df.sort_values(by=get_sf_columns(cont['deals_pull'][0]['parameters']))

    history = {}

    for index, row in my_df.iterrows():
        if not pandas.isnull(row['amount']):
            if row['deal-type'] == 'Покупка':
                if row[get_rowc(row, 'asset-code')] in history:
                    history[row[get_rowc(row, 'asset-code')]].append((history[row[get_rowc(row, 'asset-code')]][-1][0] + float(row['amount']), row['index']))
                else:
                    history[row[get_rowc(row, 'asset-code')]] = [(float(row['amount']), row['index'])]
            else:
                if row[get_rowc(row, 'asset-code')] in history:
                    history[row[get_rowc(row, 'asset-code')]].append((history[row[get_rowc(row, 'asset-code')]][-1][0] - float(row['amount']), row['index']))
                else:
                    history[row[get_rowc(row, 'asset-code')]] = [(-float(row['amount']), row['index'])]
                    
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
                val_column = 'profit' if 'profit' in cur_op else 'total'
                if cur_op['deal-type'] == 'Покупка':
                    if val_column == 'profit':
                        pro = float(cur_op[val_column])
                    else:
                        pro = - float(cur_op[val_column])
                else:
                    pro = float(cur_op[val_column])
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
            group_list['analytics'] = group[1]
            new_pull.append(group_list.copy())
            
    group_keys = []
    for key in cont['deals_pull'][0]['parameters']:
        if key['key'] not in ['date', 'amount', 'time', 'total']:
            if key['type'] == 'string':
                group_keys.append(key['key'])
                
    pull_df = pandas.DataFrame(new_pull)
    
    group_keys = [col for col in group_keys if col in pull_df.columns]
    
    if groupKeys is not None:
        group_keys = [col for col in group_keys if col in groupKeys.split(',')]
        
    if len(groupKeys) == 0:
        return {}
    
    count_plus = lambda x: x[x >= 0.0].count()
    count_plus.__name__ ='count_plus'
    count_minus = lambda x: x[x < 0.0].count()
    count_minus.__name__ ='count_minus'
    response = pull_df.groupby(group_keys).agg({
        'analytics': ['sum', count_plus, count_minus]
    })
    
    response.columns = ['_'.join(col).strip() for col in response.columns.values]
    
    return response.reset_index().to_dict(orient="records")
    
@rest_api.route('/api/rec/get')
def get_recs():
    request_args = {}
    if request.args:
        request_args = request.args
        print(request_args)
    rec_args = {k: v for k, v in request_args.items() if k in ['startDate', 'endDate', 'groupKeys']}
    return get_recomendations(**rec_args)
    
@rest_api.route('/api/stat/get')
def get_stats():
    request_args = {}
    if request.args:
        request_args = request.args
        print(request_args)
    rec_args = {k: v for k, v in request_args.items() if k in ['startDate', 'endDate', 'groupKeys']}
    get_rec = pandas.DataFrame.from_records(get_recomendations(**rec_args))
    
    result_data = {}
    
    parameters = get_rec.columns[:-3]
    for par in parameters:
        par_res = get_rec[[par, 'analytics_count_minus', 'analytics_count_plus', 'analytics_sum']].groupby(by=par).sum()
        par_res['value'] = par_res['analytics_count_minus'] + par_res['analytics_count_plus']
        par_res['accuracy'] = par_res['analytics_count_plus'] / par_res['value']
        par_res['profit'] = par_res['analytics_sum']
        result_data[par] = par_res[['value', 'accuracy', 'profit']].to_dict(orient="index")
    
    return result_data
    
@rest_api.route('/api/data_parameters/get')
def get_data_parameters():
    with open('db.json', 'r') as file:
        cont = json.loads(file.read())
    if 'deals_pull' not in cont:
        return {}
    return cont['deals_pull'][0]['parameters']