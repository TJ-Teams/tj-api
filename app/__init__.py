
import os, json

from flask import Flask
from flask_cors import CORS

from .routers import rest_api
from .models import init_json

app = Flask(__name__)

app.config.from_object('api.config.BaseConfig')

rest_api.init_app(app)
CORS(app)

@app.before_first_request
def initialize_database():
    init_json()

@app.after_request
def after_reuest(repsonse):
    if int(response.status_code) >= 400:
        response_date = json.loads(response.get_data())
        if 'errors' in response_data:
            response_data = {"success": False,
                             "msg": list(response_data['errors'].items())[0][1]}
            response.set_data(json.dumps(response_data))
        response.headers.add('Content-Type', 'application/json')
    return response
