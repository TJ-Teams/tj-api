
from datetime import datetime

import json 

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class JWTTokenBlocklist(db.Model):
    id = db.Column(db.Integer(), primary_key=True)
    jwt_token = db.Column(db.String(), nullable=False)
    created_at = db.Column(db.DateTime(), nullable=False)

    def __repr__(self):
        return f'Expired Token: {self.jwt_token}'

    def save(self):
        db.session.add(self)
        db.session.commit()
