import hug
import sqlite_utils
import configparser
import logging.config
import sqlite3
import json
import redis
import os
import socket
import requests

service = 'likes'
r = redis.StrictRedis(host='localhost', port=6379, db=0)
config = configparser.ConfigParser()
config.read('./etc/api.ini')
logging.config.fileConfig(config["logging"]["config"], disable_existing_loggers=False)

# @hug.directive()
# def patterns(section=service, key="services", **kwargs):
#     return json.loads(config.get(section, key))

@hug.directive()
def sqlite(section="sqlite", key="dbfile", **kwargs):
    dbfile = config[section][key]
    return sqlite_utils.Database(dbfile)

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

@hug.get('/user/likes/')
def userLikes(response,
    username: hug.types.text,
    db: sqlite,
    logger: log
):
    return {"Posts user likes": r.smembers(username), "username": username}

@hug.get('/top/likes/')
def topLikes(db: sqlite,logger: log):
    return {"Top posts": r.zrange('Popular posts', 0, 10-1, desc=True, withscores=True)}

@hug.get('/post/likes')
def likes(
    response,
    posts_id: hug.types.number,
    db: sqlite,
    logger: log
):
    return {"Number of likes": r.get(posts_id), "post_id": posts_id}

@hug.post("/new/likes", status=hug.falcon.HTTP_201)
def like(
    response,
    username: hug.types.text,
    posts_id: hug.types.number,
    db: sqlite,
):
    likes = db["likes"]

    like = {
        "posts_id": posts_id,
        "username": username
    }

    r.set(posts_id, '0', nx=True)
    r.incr(posts_id)

    r.sadd(username, posts_id)

    posts = db["posts"]
    posts_db = sqlite3.connect('./var/posts.db')
    c = posts_db.cursor()
    c.execute('SELECT message FROM posts WHERE id=? ', (posts_id,))
    user_post = c.fetchone()

    r.zadd('Popular posts', {user_post[0]: r.get(posts_id)})
    return like
