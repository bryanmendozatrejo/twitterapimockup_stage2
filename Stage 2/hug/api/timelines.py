import hug
import sqlite_utils
import configparser
import logging.config
import datetime
import requests
import sqlite3
import base64
import http.client
import json
from requests.auth import HTTPBasicAuth
import os
import socket
import requests

service = 'posts'
config = configparser.ConfigParser()
config.read('./etc/api.ini')
logging.config.fileConfig(config["logging"]["config"], disable_existing_loggers=False)

@hug.directive()
def sqlite(section="sqlite", key="dbfile1", **kwargs):
    dbfile1 = config[section][key]
    return sqlite_utils.Database(dbfile1)

@hug.directive()
def log(name=__name__, **kwargs):
    return logging.getLogger(name)

@hug.startup()
def on_start_up(api=None):
    dict = json.loads(config.get(service, "services"))
    port = os.environ['PORT']
    url = socket.getfqdn("http://localhost:"+port)
    for serv in dict:
        r = requests.get(f'http://localhost:5500/service/exists?service_name={service}&url={url + serv}&http={dict[serv]}')
        if r.status_code != 201:
            r = requests.post(f'http://localhost:5500/new/service?service_name={service}&url={url + serv}&http={dict[serv]}')

@hug.get('/health-check')
def health_check(response):
    return requests.get(f'http://localhost:5500/health-check?service_name={service}').json()

globUser = ''

def exists(username, password):
    global globUser
    r = requests.get(f'http://localhost:5500/service/exists?service_name=users&url=http://localhost:5000/users/verify&http=GET')
    if r.status_code == 200:
        s = requests.get(f'http://localhost:5000/users/verify?username={username}&password={password}')
        if s.status_code == 200:
            globUser = username
            return True
        else:
            globUser = ''
            return False
    else:
        response.status = hug.falcon.HTTP_502
        return {"error": "Service instance doesn't exist!"}


@hug.post("/posts/new/", status=hug.falcon.HTTP_201, requires=hug.authentication.basic(exists))
def addPost(
    response,
    message : hug.types.text,
    db: sqlite,
):
    username = globUser
    posts = db["posts"]

    post = {
        "username" : username,
        "message": message,
        "timestamp":  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "repost": '',
    }

    try:
        posts.insert(post)
        post["id"] = posts.last_pk
        return post

    except Exception as e:
        response.status = hug.falcon.HTTP_409
        return {"error": str(e)}

@hug.get("/repost/", status=hug.falcon.HTTP_201, requires=hug.authentication.basic(exists))
@hug.post("/repost/", status=hug.falcon.HTTP_201, requires=hug.authentication.basic(exists))
def rePost(
    response, request,
    id: hug.types.number,
    db: sqlite,
):
    username = globUser
    posts = db["posts"]
    posts_db = sqlite3.connect('./var/posts.db')
    c = posts_db.cursor()
    c.execute('SELECT message FROM posts WHERE id=? ', (id,))
    user_post = c.fetchone()
    port = os.environ['PORT']
    url = socket.getfqdn("http://localhost:"+port)
    post = {
        "username" : username,
        "message": user_post[0],
        "timestamp":  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "repost": url + '/allposts' + f'?id={id}'
    }

    try:
        posts.insert(post)
        post["id"] = posts.last_pk
        return post

    except Exception as e:
        response.status = hug.falcon.HTTP_409
        return {"error": str(e)}

@hug.get('/allposts/', status=hug.falcon.HTTP_200)
def getPublicTimeline(request, db: sqlite, logger: log):
    posts = db["posts"]
    values = []

    try:
        if "id" in request.params:
            values.append(request.params['id'])
            logger.debug('WHERE "%s", %r', "id = ?", values)
            logger.debug('ORDER BY "%s"', 'timestamp desc')
            return {"posts": posts.rows_where("id = ?", request.params['id'], order_by= 'timestamp desc')}
        else:
            return {"posts": posts.rows_where(order_by= 'timestamp desc')}

    except sqlite_utils.db.NotFoundError:
        response.status = hug.falcon.HTTP_404
        return {"error": str(e)}

@hug.get("/posts/{username}", status=hug.falcon.HTTP_200)
def getUserTimeline(
    username: hug.types.text,
    response,
    db: sqlite,
    logger: log
):
    posts = db["posts"]
    values = []

    try:
        values.append(username)
        logger.debug('WHERE "%s", %r', "username = ?", values)
        logger.debug('ORDER BY "%s"', 'timestamp desc')
        return {"posts": posts.rows_where("username = ?", values, order_by='timestamp desc')}
    except sqlite_utils.db.NotFoundError:
        response.status = hug.falcon.HTTP_404
        return {"error": str(e)}

@hug.get("/posts/home/", requires=hug.authentication.basic(exists))
def getHomeTimeline(response, db: sqlite, logger: log):
    username = globUser
    posts = db["posts"]
    post = []

    conditions = []
    values = []
    data = []
    r = requests.get(f'http://localhost:5500/service/exists?service_name=users&url=http://localhost:5000/followers&http=GET')
    if r.status_code == 200:
        data = requests.get(f'http://localhost:5000/followers?username={username}').json()
        dataSize = len(data["followers"])
    else:
        response.status = hug.falcon.HTTP_502
        return {"error": "Service instance doesn't exist!"}

    for i in range(dataSize):
        conditions.append("username = ?")
        values.append(data["followers"][i]["friend_username"])

    if conditions:
        where = " OR ".join(conditions)
        logger.debug('WHERE "%s", %r', where, values)
        logger.debug('ORDER BY "%s"', 'timestamp desc')
        post.append(posts.rows_where(where, values, order_by= 'timestamp desc'))
        return {"posts": post}
    else:
        response.status = hug.falcon.HTTP_401
        return {"error: No followers"}
