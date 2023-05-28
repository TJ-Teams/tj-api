
from datetime import datetime

import sqlite3

import json 
import os

import uuid

def init_json():
    if not os.path.isfile('db.json'):
        base_json = {
                'deals_pull': []
        }
        with open('db.json', 'w') as file:
            file.write(json.dumps(base_json))

def init_db():

    GLOBAL_DB = sqlite3.connect('mdb.db')
    cursor = GLOBAL_DB.cursor()
    print('База данных создана и успешно подключена к SQLite')
    
    cursor.execute('create table if not exists users (fname text not null, sname text not null, email text not null primary key, passw text not null, json_name text not null)')
    GLOBAL_DB.commit()
    print('Таблица users создана')
    
    cursor.close()
    
def create_user(fname, sname, email, passw):
    if fname and sname and email and passw:
        GLOBAL_DB = sqlite3.connect('mdb.db')
        cursor = GLOBAL_DB.cursor()
        
        cursor.execute('select * from users where email = ?', (email,))
        row = cursor.fetchone()
        if row:
            return 2
        
        json_name = str(uuid.uuid4())
        cursor.execute('insert into users(fname, sname, email, passw, json_name) values (?, ?, ?, ?, ?)', (fname, sname, email, passw, json_name))
        GLOBAL_DB.commit()
        
        default_data = {
            "parameters": [],
            "deals": {}
        }
        
        filename = 'deals/' + json_name + '.json'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as file:
            file.write(json.dumps(default_data))
        
        cursor.close()
        
        return 0
    else:
        return 1
        
def login_user(email, passw):
    if email and passw:
        GLOBAL_DB = sqlite3.connect('mdb.db')
        cursor = GLOBAL_DB.cursor()
        
        cursor.execute('select * from users where email = ? and passw = ?', (email, passw))
        row = cursor.fetchone()
        if row:
            return 0
        else:
            return 2
            
    else:
        return 1
        
    
def get_json_path(email):
    GLOBAL_DB = sqlite3.connect('mdb.db')
    cursor = GLOBAL_DB.cursor()
    cursor.execute('select json_name from users where email = ?', (email,))
    row = cursor.fetchone()
    return str(row[0])
    
def get_user_info(email):
    GLOBAL_DB = sqlite3.connect('mdb.db')
    cursor = GLOBAL_DB.cursor()
    cursor.execute('select fname, sname, email from users where email = ?', (email,))
    row = cursor.fetchone()
    return {'first_name': row[0], 'second_name': row[1], 'email': row[2]}