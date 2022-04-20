import os
from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
from datetime import date

TODAY = date.today()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Prediction(db.Model):
    __tablename__ = 'predictions'
    game_id = db.Column(db.String, primary_key=True)
    date = db.Column(db.Date)
    venue = db.Column(db.String)
    home_team = db.Column(db.String)
    away_team = db.Column(db.String)
    start_time = db.Column(db.DateTime)
    home_win = db.Column(db.Boolean)
    away_prob = db.Column(db.Float)
    home_prob = db.Column(db.Float)


@app.route('/')
def index():
    games = Prediction.query.filter_by(date=TODAY).all()
    if len(games) > 0:
        return render_template('index.html', games_today=True, games=games)
    else:
        return render_template('index.html', games_today=False, games=games)


if __name__ == '__main__':
    app.run(debug=True)
