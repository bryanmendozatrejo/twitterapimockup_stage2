import logging
import threading
import time
import hug
import boto3
import os
import socket
import requests
import configparser
import logging.config
import sqlite_utils
import sqlite3
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import concurrent.futures

config = configparser.ConfigParser()
config.read('./etc/api.ini')
logging.config.fileConfig(config["logging"]["config"], disable_existing_loggers=False)

@hug.directive()
def sqlite(section="sqlite", key="dbfile2", **kwargs):
    dbfile2 = config[section][key]
    return sqlite_utils.Database(dbfile2)

@hug.directive()
def sqlite1(section="sqlite", key="dbfile3", **kwargs):
    dbfile3 = config[section][key]
    return sqlite_utils.Database(dbfile3)

@hug.directive()
def log(name=__name__, **kwargs):
    return logging.getLogger(name)

@hug.get('/services/')
def services(db: sqlite):
    return{'services': db['services'].rows}

@hug.get("/service/exists", status=hug.falcon.HTTP_201)
def serviceExists(
    response,
    service_name: hug.types.text,
    url: hug.types.text,
    http: hug.types.text,
    db: sqlite,
):
    try:
        services_db = sqlite3.connect('./var/services.db')
        c = services_db.cursor()
        c.execute('SELECT service_name, url, http  FROM services WHERE service_name=? AND url=? AND http=?', (service_name,url,http))
        serv = c.fetchone()

        if serv:
            response.status = hug.falcon.HTTP_201
            return json.dumps({"service_name": service_name,"url": url, "http": http})

        else:
            response.status = hug.falcon.HTTP_401
            return {"error" : "Service doesn't exist"}

    except Exception as e:
        response.status = hug.falcon.HTTP_409
        return {"error": str(e)}

@hug.post("/new/service", status=hug.falcon.HTTP_201)
def newService (
    response,
    service_name: hug.types.text,
    url: hug.types.text,
    http: hug.types.text,
    db: sqlite,
):
    services = db["services"]

    service = {
        "service_name": service_name,
        "url": url,
        "http": http,
    }

    try:
        services.insert(service)
        service["id"] = services.last_pk
    except Exception as e:
        response.status = hug.falcon.HTTP_409
        return {"error": str(e)}

    response.set_header("Location", f"/services/{service['id']}")
    return service

@hug.get('/health-check')
def healths(request, db: sqlite1, logger: log):
    healths_db = db['healths']
    values = []

    if "service_name" in request.params:
        values.append(request.params['service_name'])
        logger.debug('WHERE "%s", %r', "service_name = ?", values)
        return {request.params['service_name'] + "_health": healths_db.rows_where("service_name = ?", values)}

    return{'urls_health': healths_db.rows}

@hug.post("/urls/health/", status=hug.falcon.HTTP_201)
def urls_health(
    response,
    service_name: hug.types.text,
    http: hug.types.text,
    url: hug.types.text,
    status_code: hug.types.number,
    db: sqlite1,
):
    urls_health = db["healths"]

    url_health = {
        "service_name": service_name,
        'http': http,
        "url": url,
        "status_code": status_code
    }

    try:
        urls_health.insert(url_health)
        url_health["id"] = services.last_pk
    except Exception as e:
        response.status = hug.falcon.HTTP_409
        return {"error": str(e)}

def health_check(url):
    healths_db = sqlite3.connect('./var/healths.db')
    c = healths_db.cursor()
    c.execute('SELECT status_code FROM healths WHERE url=?', (url,))
    status_code = c.fetchone()
    services_db = sqlite3.connect('./var/services.db')
    s = services_db.cursor()
    s.execute('SELECT service_name FROM services WHERE url=?', (url,))
    urls = s.fetchone()
    if status_code[0] != 200 and urls[0] != '':
        c.execute('DELETE FROM healths WHERE url=?',(url,))
        s.execute('DELETE FROM services WHERE url=?',(url,))
        healths_db.commit()
        services_db.commit()


class thread_lock(object):
    def __init__(self, start = 0):
        self.value = start
        self.lock = threading.Lock()

    def locked_update(self, url):
        logging.debug("Waiting for a lock")
        self.lock.acquire()
        try:
            logging.debug("Acquired a lock")
            health_check(url)
            self.value = self.value + 1
        finally:
            logging.debug("Released a lock")
            self.lock.release()

def thread_function(lck):
    serv = []
    urls = []
    http = []

    logging.info("Thread starting")

    posts_db = sqlite3.connect('./var/services.db')
    c = posts_db.cursor()
    for row in c.execute(f'SELECT service_name FROM services'): serv.append(row[0])
    for row in c.execute(f'SELECT url FROM services'): urls.append(row[0])
    for row in c.execute(f'SELECT http FROM services'): http.append(row[0])
    for i in range(len(serv)):
        if urls[i] != 'http://localhost:5500':
            r = 0
            if http[i] == "GET":
                r = requests.get(urls[i])
            elif http[i] == "POST":
                r = requests.post(urls[i])
            elif http[i] == "DELETE":
                r = requests.delete(urls[i])
            if r.status_code:
                requests.post(f'http://localhost:5500/urls/health/?service_name={serv[i]}&url={urls[i]}&http={http[i]}&status_code={r.status_code}')
                lck.locked_update(urls[i])

    time.sleep(2)
    logging.info("Thread finishing")
    time.sleep(30)
    x = threading.Thread(target=thread_function, args=(thread_lock(),), daemon=True)
    x.start()


@hug.startup()
def on_start_up(api=None):
    lck = thread_lock()
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

    x = threading.Thread(target=thread_function, args=(lck,), daemon=True)
    x.start()
