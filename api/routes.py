
from flask import request
from flask import Flask
#from flask_restx import Api

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
