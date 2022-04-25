import hug
import sqlite_utils
import configparser
import logging.config
import sqlite3
import json
import boto3
from decimal import Decimal
from pprint import pprint
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import ast
import uuid
import os
import socket
import requests

service = 'polls'
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

@hug.get('/allresults/')
def get_all_results(dynamodb=None):
    dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table('Results')
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return response['Items']

@hug.get('/vote/results/')
def get_results(response, polls_id: hug.types.text, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Results')
    while True:
        if not table.global_secondary_indexes or table.global_secondary_indexes[0]['IndexStatus'] != 'ACTIVE':
            print('Waiting for index to backfill...')
            time.sleep(5)
            table.reload()
        else:
            break
    resp = table.query(
        IndexName="polls_title_secondary",
        KeyConditionExpression=Key('polls_id').eq(polls_id),
    )
    print("The query returned the following items:")
    return {"Results": resp['Items']}

@hug.post("/poll/vote", status=hug.falcon.HTTP_201)
def votePoll(
    user: hug.types.text,
    polls_id: hug.types.text,
    choice: hug.types.number,
    dynamodb=None):

    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Polls')
    table1 = dynamodb.Table('Voters')
    table2 = dynamodb.Table('Results')

    try:
        if(((table1.get_item(Key={'user': user, 'polls_id': polls_id})['Item'])['user'])==user):
            return {"Error": "User has already voted."}
    except KeyError:
        try:
            polls_title = (table.get_item(Key={'polls_id': polls_id})['Item'])['polls_title']
        except KeyError:
            return {"Error": "Poll_id does not exist."}
        try:
            polls_response = (table.get_item(Key={'polls_id': polls_id})['Item'])['responses'][choice-1]
            polls_responses = (table.get_item(Key={'polls_id': polls_id})['Item'])['responses']
        except IndexError:
            return {"Error": "Response does not exist."}
        try:
            table2.get_item(Key={'polls_id': polls_id, 'response': polls_response})['Item']
            increase_votes(polls_id, polls_response, polls_responses, 1, 1)
        except KeyError:
            put_results(polls_id, polls_title, polls_response, polls_responses, 1, 1)

        votes = put_votes(user, polls_id, polls_title, polls_response)
        return votes

@hug.get('/polls/')
def get_polls(polls_id: hug.types.text, dynamodb=None):
    polls = get_polls_id(polls_id)
    if polls == []:
         return {"Error": "There are no polls with this id."}
    else:
        return polls

@hug.post("/create/poll", status=hug.falcon.HTTP_201)
def createPoll(
    polls_title: hug.types.text,
    username: hug.types.text,
    responses: hug.types.delimited_list(using=','),
    dynamodb=None):
    # hug.types.delimited_list(using=',')
    polls = put_polls(polls_title, username, responses)
    if polls == []:
         response.status = hug.falcon.HTTP_401
         return {"Error": "Could not add Item."}
    else:
        return polls

#-------------------------------------------------------------------------------------------
# dynamodb functions
#-------------------------------------------------------------------------------------------
def get_results_id(polls_id, response, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Results')

    try:
        return table.get_item(Key={'polls_id': polls_id, 'response': response})['Item']
    except KeyError:
        return []

def put_results(polls_id, polls_title, response, responses, total_votes_for_response, total_votes, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Results')
    for resp in responses:
        Item={
             'polls_id': polls_id,
             'polls_title': polls_title,
             'response': resp,
             'total_votes_for_response': 0,
             'total_votes': total_votes
        }
        table.put_item(Item=Item)
    Item={
         'polls_id': polls_id,
         'polls_title': polls_title,
         'response': response,
         'total_votes_for_response': total_votes_for_response,
         'total_votes': total_votes
    }
    table.put_item(Item=Item)

def increase_votes(polls_id, response, responses, total_votes_for_response, total_votes, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Results')

    table.update_item(
        Key={'polls_id': polls_id, 'response': response},
        UpdateExpression="set total_votes_for_response = total_votes_for_response + :val",
        ExpressionAttributeValues={':val': Decimal(total_votes_for_response)},
        ReturnValues="UPDATED_NEW"
    )

    for resp in responses:
        table.update_item(
            Key={'polls_id': polls_id, 'response': resp},
            UpdateExpression="set total_votes = total_votes + :val",
            ExpressionAttributeValues={':val': Decimal(total_votes)},
            ReturnValues="UPDATED_NEW"
        )

def put_votes(user, polls_id, polls_title, polls_response, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Voters')
    Item={
         'user': user,
         'polls_id': polls_id,
         'polls_title': polls_title,
         'response': polls_response
    }
    try:
        response = table.put_item(Item=Item)
        return Item
    except ClientError as e:
        print(e.response['Error']['Message'])
        return []

def get_polls_id(polls_id, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Polls')

    try:
        return table.get_item(Key={'polls_id': polls_id})['Item']
    except KeyError:
        return []

def put_polls(polls_title, username, responses, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Polls')
    Item={
         'polls_id': str(uuid.uuid4()),
         'polls_title': polls_title,
         'username': username,
         'responses': responses
    }
    try:
        response = table.put_item(Item=Item)
        return Item
    except ClientError as e:
        print(e.response['Error']['Message'])
        return []

#-------------------------------------------------------------------------------------------
# Test Functions
#-------------------------------------------------------------------------------------------
# # test purposes only!
# @hug.get('/allpolls/')
# def get_all_polls( dynamodb=None):
#     dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
#     table = dynamodb.Table('Polls')
#     response = table.scan()
#     data = response['Items']
#     while 'LastEvaluatedKey' in response:
#         response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
#         data.extend(response['Items'])
#     return response['Items']

#-------------------------------------------------------------------------------------------
# # test purposes only!
# @hug.get('/allresults/')
# def get_all_results(dynamodb=None):
#     dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
#     table = dynamodb.Table('Results')
#     response = table.scan()
#     data = response['Items']
#     while 'LastEvaluatedKey' in response:
#         response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
#         data.extend(response['Items'])
#     return response['Items']

#-------------------------------------------------------------------------------------------
# #for testing use only
# @hug.get('/allvotes/')
# def get_all_votes( dynamodb=None):
#     dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
#     table = dynamodb.Table('Voters')
#     response = table.scan()
#     data = response['Items']
#     while 'LastEvaluatedKey' in response:
#         response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
#         data.extend(response['Items'])
#     return response['Items']
#
# #for testing use only
# def get_votes_id(user, polls_id, dynamodb=None):
#     if not dynamodb:
#         dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
#
#     table = dynamodb.Table('Voters')
#
#     try:
#         return table.get_item(Key={'user': user, 'polls_id': polls_id})['Item']
#     except KeyError:
#         return []
#
# #for testing use only
# @hug.get('/votes/')
# def get_votes(user: hug.types.text, polls_id: hug.types.text, dynamodb=None):
#     votes = get_votes_id(user, polls_id)
#     if votes == []:
#          return {"Error: There are no polls or user with this vote."}
#     else:
#         return votes
#-------------------------------------------------------------------------------------------
