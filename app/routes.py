
from flask import request
from flask_restx import Api

from .config import BaseConfig
import requests


rest_api = Api(version="1.0", title="TJ API")


@rest_api.route('/api/data/add', methods=['PUT'])
def put_data():
    data = request.get_json()
    with open('db.json', 'r+') as file:
        db_data = json.loads(file.read())
        file.seek(0)

        if len(db_data['deals_pull']) == 0:
            db_data['deals_pull'] = [data]
        elif:
            db_data['deals_pull'][0]['parameters'] = data['parameters']
            db_data['deals_pull'][0]['deals'] = {key: data['deals'].get(key, db_data['deals_pull'][0]['deals'][key]) for key in db_data['deals_pull'][0]['deals']} 

        file.write(json.dumps(db_data))
        file.truncate()
    return {
            "success": True,
           }, 200

@rest_api.route('/api/data/get', method=['GET'])
def get_data():
    with open('db.json', 'r') as file:
        data = json.loads(file.read())['deals_pull'][0]
    return data, 200
