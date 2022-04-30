from flask import Blueprint, render_template, request
from flask_login import login_required, current_user
import pymysql
import csi3335sp2022 as cfg

views = Blueprint('views', __name__)


@views.route('/', methods=['GET', 'POST'])
@login_required
def home():
    con = pymysql.connect(host=cfg.mysql['location'], user=cfg.mysql['user'], password=cfg.mysql['password'],
                          db=cfg.mysql['database'])
    with con:
        cur = con.cursor()
        sql='SELECT DISTINCT name FROM team'
        cur.execute(sql)
        results = cur.fetchall()
    print(request.form.get('teamname'))


    return render_template("home.html", user=current_user,results=results)


@views.route('/year', methods=['GET', 'POST'])
@login_required
def selectyear():
    teamname=request.form.get('name')

    con = pymysql.connect(host=cfg.mysql['location'], user=cfg.mysql['user'], password=cfg.mysql['password'],
                          db=cfg.mysql['database'])
    with con:
        cur = con.cursor()
        sql = 'SELECT DISTINCT yearid FROM team WHERE name=%s'
        cur.execute(sql,teamname)
        results = cur.fetchall()
    return render_template("year.html", user=current_user, results=results)

@views.route('/players', methods=['GET', 'POST'])
@login_required
def players():
    return render_template("players.html",user=current_user)
