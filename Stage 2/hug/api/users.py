import hug
import sqlite_utils
import configparser
import logging.config
import sqlite3
import json
import os
import socket
import requests

service = 'users'
config = configparser.ConfigParser()
config.read('./etc/api.ini')
logging.config.fileConfig(config["logging"]["config"], disable_existing_loggers=False)

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

@hug.get('/users/')
def users(db: sqlite):
    return{'users': db['users'].rows}

@hug.post("/users/new", status=hug.falcon.HTTP_201)
def createUser(
    response,
    username: hug.types.text,
    bio: hug.types.text,
    email: hug.types.text,
    password: hug.types.text,
    db: sqlite,
):
    users = db["users"]

    user = {
        "username": username ,
        "bio": bio ,
        "email": email ,
        "password": password ,
    }

    try:
        users.insert(user)
        user["id"] = users.last_pk
    except Exception as e:
        response.status = hug.falcon.HTTP_409
        return {"error": str(e)}

    response.set_header("Location", f"/users/{user['id']}")
    return user

@hug.get('/users/verify')
def verifyUser(response, request,
    username: hug.types.text,
    password: hug.types.text,
    db: sqlite, logger: log):
    try:
        posts_db = sqlite3.connect('./var/users.db')
        c = posts_db.cursor()
        logger.debug('WHERE "%s", %r, %r', "username = ? AND password = ?", username, password)
        c.execute(f'SELECT username, password FROM users WHERE username="{username}" and password="{password}"')
        user = c.fetchone()
        if user:
            return json.dumps({"username": username,"password": password})

        else:
            response.status = hug.falcon.HTTP_401
            return {"error: User doesn't exist"}

    except Exception as e:
        response.status = hug.falcon.HTTP_409
        return {"error": str(e)}

@hug.get('/followers/')
def followers(response, request, db: sqlite, logger: log):
    followers = db["followers"]
    values = []

    try:
        if "username" in request.params:
            values.append(request.params['username'])
            logger.debug('WHERE "%s", %r', "username = ?", values)
            return {"followers": followers.rows_where("username = ?", values)}
        else:
            return{'followers': db['followers'].rows}

    except sqlite_utils.db.NotFoundError:
        response.status = hug.falcon.HTTP_404
        return {"error": str(e)}

@hug.post("/followers/new/", status=hug.falcon.HTTP_201)
def addFollower(
    response,
    username: hug.types.text,
    friend_username : hug.types.text,
    db: sqlite,
):
    followers = db["followers"]

    follower = {
        "username": username,
        "friend_username": friend_username,
    }

    try:
        users_db = sqlite3.connect('./var/users.db')
        c = users_db.cursor()
        c.execute('SELECT username FROM users WHERE username=?', (username,))
        user1 = c.fetchone()
        c.execute('SELECT username FROM users WHERE username=?', (friend_username,))
        user2 = c.fetchone()

        if user1 and user2:
            if (username == user1[0] and friend_username == user2[0] and user1[0] != user2[0]):
                followers.insert(follower)
                follower["id"] = followers.last_pk
                response.set_header("Location", f"/followers/{follower['id']}")
                return follower
            else:
                response.status = hug.falcon.HTTP_409
                return {"error: Can't follow yourself"}
        else:
            response.status = hug.falcon.HTTP_409
            return {"error: User doesn't exist"}
    except Exception as e:
        response.status = hug.falcon.HTTP_409
        return {"error": str(e)}

@hug.delete("/unfollow/", status=hug.falcon.HTTP_201)
def removeFollower(
    response,
    username: hug.types.text,
    friend_username : hug.types.text,
    db: sqlite,
):
    followers = db["followers"]

    unfollow = {
        "username": username,
        "friend_username": friend_username,
    }

    followers_db = sqlite3.connect('./var/users.db')
    f = followers_db.cursor()
    f.execute('SELECT id FROM followers WHERE username=? AND friend_username=?', (username,friend_username))
    id = f.fetchone()

    if id:
        f.execute('DELETE FROM followers WHERE id=?',(id[0],))
        followers_db.commit()
        return unfollow
    else:
        response.status = hug.falcon.HTTP_409
        return {"error: User doesn't exist"}
