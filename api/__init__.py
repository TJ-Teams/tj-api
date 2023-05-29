
import os, json

from flask import Flask
from flask_cors import CORS

from .routes import rest_api, db
from .models import init_db

app = rest_api

CORS(app)

@app.before_first_request
def initialize_database():
    db.create_all()
    init_db()