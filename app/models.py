
from datetime import datetime

import json 
import os

def init_json():
    if !os.path.isfile('db.json'):
        base_json = {
                'deals_pull': []
        }
        with open('db.json', 'w') as file:
            file.write(json.dumps(base_json))


class JWTTokenBlocklist(db.Model):
    id = db.Column(db.Integer(), primary_key=True)
    jwt_token = db.Column(db.String(), nullable=False)
    created_at = db.Column(db.DateTime(), nullable=False)

    def __repr__(self):
        return f'Expired Token: {self.jwt_token}'

    def save(self):
        db.session.add(self)
        db.session.commit()
