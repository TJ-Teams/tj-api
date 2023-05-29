
from flask import request
from flask import jsonify
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from datetime import datetime
from datetime import timedelta
from datetime import timezone

from flask_jwt_extended import create_access_token
from flask_jwt_extended import get_jwt
from flask_jwt_extended import get_jwt_identity
from flask_jwt_extended import jwt_required
from flask_jwt_extended import JWTManager

import pandas
import json
import sys
from .config import BaseConfig
import requests

from .models import create_user, get_json_path, login_user, get_user_info

ACCESS_EXPIRES = timedelta(hours=1)

rest_api = Flask(__name__)
rest_api.config["JWT_SECRET_KEY"] = "f30f409Uf3jjf0j4J09jf0jfwfmlbwdfvdsdc324f3g556h567jfr" 
rest_api.config["JWT_ACCESS_TOKEN_EXPIRES"] = ACCESS_EXPIRES
jwt = JWTManager(rest_api)

rest_api.config["SQLALCHEMY_DATABASE_URI"] = "sqlite://"
rest_api.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(rest_api)

# https://flask-jwt-extended.readthedocs.io/en/stable/blocklist_and_token_revoking.html
class TokenBlocklist(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    jti = db.Column(db.String(36), nullable=False, index=True)
    created_at = db.Column(db.DateTime, nullable=False)

@jwt.token_in_blocklist_loader
def check_if_token_revoked(jwt_header, jwt_payload: dict) -> bool:
    jti = jwt_payload["jti"]
    token = db.session.query(TokenBlocklist.id).filter_by(jti=jti).scalar()
    return token is not None

@rest_api.route("/api/users/login", methods=["POST"])
def login():
    email = request.json.get("email", None)
    password = request.json.get("password", None)
    result = login_user(email, password)
    if result != 0:
        result_mess = {
            1: 'Bad fields values',
            2: 'Email or password is wrong',
        }
        return jsonify({"msg": result_mess[result], "error_code": result}), 401
    access_token = create_access_token(identity=email)
    return jsonify(access_token=access_token)

@rest_api.route("/api/users/register", methods=["POST"])
def register():
    fname = request.json.get("first_name", None)
    sname = request.json.get("second_name", None)
    email = request.json.get("email", None)
    passw = request.json.get("password", None)
    result = create_user(fname, sname, email, passw)
    if result != 0:
        result_mess = {
            1: 'Bad fields values',
            2: 'User with such email is already exists',
        }
        return jsonify({"msg": result_mess[result], "error_code": result}), 401
    access_token = create_access_token(identity=email)
    return jsonify(access_token=access_token)
    
@rest_api.route("/api/users/logout", methods=["DELETE"])
@jwt_required()
def modify_token():
    jti = get_jwt()["jti"]
    now = datetime.now(timezone.utc)
    db.session.add(TokenBlocklist(jti=jti, created_at=now))
    db.session.commit()
    return jsonify(msg="JWT revoked")

@rest_api.route('/api/users/info', methods=['GET'])
@jwt_required()
def get_info():
    email = get_jwt_identity()
    return jsonify(get_user_info(email))

@rest_api.route('/api/data/set', methods=['PUT'])
@jwt_required()
def put_data1():
    json_name = get_json_path(get_jwt_identity())
    filename = 'deals/' + json_name + '.json'
    data = request.get_json()
    with open(filename, 'w') as file:
        file.write(json.dumps(data, indent=4))
    return {
            "success": True,
           }, 200

@rest_api.route('/api/data/get', methods=['GET'])
@jwt_required()
def get_data1():
    json_name = get_json_path(get_jwt_identity())
    filename = 'deals/' + json_name + '.json'
    with open(filename, 'r') as file:
        data = json.loads(file.read())
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

def get_recomendations(startDate=None, endDate=None, groupKeys=None, jsonn=None):
    filename = 'deals/' + jsonn + '.json'
    with open(filename, 'r') as file:
        cont = json.loads(file.read())
    if len(cont['deals']) == 0:
        return []
        
    my_df_r = pandas.DataFrame.from_dict(cont['deals'], orient='index')
    my_df_r = my_df_r.reset_index()
    
    dfs_list = []
    pdfs_list = []
    
    if 'provider-type' in my_df_r.columns:
        for group_name, group_df in my_df_r.groupby('provider-type'):
            dfs_list.append(group_df)
    else:
        dfs_list = [my_df_r]
        
    for my_df in dfs_list:
    
        if 'date' in my_df.columns:
            my_df['date'] = pandas.to_datetime(my_df['date'], format='%Y-%m-%d')
            if startDate is not None:
                my_df = my_df[my_df['date'] >= pandas.to_datetime(startDate, format='%Y-%m-%d')]
            if endDate is not None:
                my_df = my_df[my_df['date'] <= pandas.to_datetime(endDate, format='%Y-%m-%d')]

        my_df = my_df.sort_values(by=get_sf_columns(cont['parameters']))

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
               
                    
        pull_df = pandas.DataFrame(new_pull)
        
        pdfs_list.append(pull_df)
    
    if len(pdfs_list) > 1:
        pull_df = pandas.concat(pdfs_list)
    else:
        pull_df = pdfs_list[0]
    
    group_keys = []
    for key in cont['parameters']:
        if key['key'] not in ['date', 'amount', 'time', 'total']:
            if key['type'] == 'string':
                group_keys.append(key['key'])
    
    group_keys = [col for col in group_keys if col in pull_df.columns]
    
    if groupKeys is not None:
        group_keys = [col for col in group_keys if col in groupKeys.split(',')]
        
    if len(group_keys) == 0:
        return []
    
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
@jwt_required()
def get_recs():
    json_name = get_json_path(get_jwt_identity())
    request_args = {}
    if request.args:
        request_args = request.args
        print(request_args)
    rec_args = {k: v for k, v in request_args.items() if k in ['startDate', 'endDate', 'groupKeys']}
    rec_args['jsonn'] = json_name
    return get_recomendations(**rec_args)
    
@rest_api.route('/api/stat/get')
@jwt_required()
def get_stats():
    json_name = get_json_path(get_jwt_identity())
    request_args = {}
    if request.args:
        request_args = request.args
        print(request_args)
    rec_args = {k: v for k, v in request_args.items() if k in ['startDate', 'endDate', 'groupKeys']}
    rec_args['jsonn'] = json_name
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
@jwt_required()
def get_data_parameters():
    json_name = get_json_path(get_jwt_identity())
    filename = 'deals/' + json_name + '.json'
    with open(filename, 'r') as file:
        cont = json.loads(file.read())
    return cont['parameters']