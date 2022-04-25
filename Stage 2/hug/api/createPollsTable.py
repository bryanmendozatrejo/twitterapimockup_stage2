import boto3
from decimal import Decimal
from pprint import pprint
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import json
import uuid
import time

def create_polls_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
        TableName='Polls',
        KeySchema=[
            {
                'AttributeName': 'polls_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'polls_id',
                'AttributeType': 'S'
            }

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    table = dynamodb.create_table(
        TableName='Voters',
        KeySchema=[
            {
                'AttributeName': 'user',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'polls_id',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'user',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'polls_id',
                'AttributeType': 'S'
            }

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    table = dynamodb.create_table(
        TableName='Results',
        KeySchema=[
            {
                'AttributeName': 'polls_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'response',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'polls_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'response',
                'AttributeType': 'S'
            }

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return 'Tables Created!'

#----------------------------------------------------------------------------------------------

def load_polls(polls, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Polls')
    for poll in polls:
        polls_id = poll['polls_id']
        polls_title = poll['polls_title']
        username = poll['username']
        responses = poll['responses']
        print("Adding poll:", polls_id, polls_title, username, responses)
        table.put_item(Item=poll)

def get_polls(polls_id, polls_title, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Polls')

    try:
        response = table.get_item(Key={'polls_id': polls_id})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

def delete_poll_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Polls')
    table.delete()

#----------------------------------------------------------------------------------------------

def load_voters(voters, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Voters')
    for voter in voters:
        user = voter['user']
        polls_id = voter['polls_id']
        polls_title = voter['polls_title']
        response = voter['response']
        print("Adding poll:", user, polls_id, polls_title, response)
        table.put_item(Item=voter)

def get_voters(user, polls_id, polls_title, response, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Voters')

    try:
        response = table.get_item(Key={'user': user, 'polls_id': polls_id})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

def delete_voter_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Voters')
    table.delete()

#----------------------------------------------------------------------------------------------

def load_results(results, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Results')
    for result in results:
        polls_id = result['polls_id']
        polls_title = result['polls_title']
        response = result['response']
        total_votes_for_response = result['total_votes_for_response']
        total_votes = result['total_votes']
        print("Adding poll:", polls_id, polls_title, response, total_votes_for_response, total_votes)
        table.put_item(Item=result)

def get_results(polls_id, polls_title, response, total_votes_for_response, total_votes, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Results')

    try:
        response = table.get_item(Key={'polls_id': polls_id, 'response': response})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

def delete_results_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Results')
    table.delete()

def add_secondary_index(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.client('dynamodb', endpoint_url="http://localhost:8000")
    try:
        resp = dynamodb.update_table(
            TableName="Results",
            AttributeDefinitions=[
                {
                    "AttributeName": "polls_id",
                    "AttributeType": "S"
                },
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": "polls_title_secondary",
                        "KeySchema": [
                            {
                                "AttributeName": "polls_id",
                                "KeyType": "HASH"
                            }
                        ],
                        "Projection": {
                            "ProjectionType": "ALL"
                        },
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1,
                        }
                    }
                }
            ],
        )
        print("Secondary index added!")
    except Exception as e:
        print("Error updating table:")
        print(e)

def get_secondary_index_results(polls_title, dynamodb=None):
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
        KeyConditionExpression=Key('polls_title').eq(polls_title),
    )
    print("The query returned the following items:")
    for item in resp['Items']:
        print(item)

#----------------------------------------------------------------------------------------------

if __name__ == '__main__':
    # delete_poll_table()
    # print("Polls table deleted.")
    # delete_voter_table()
    # print("Voters table deleted.")
    # delete_results_table()
    # print("Results table deleted.")
    tables = create_polls_table()
    with open("share/polls.json") as json_file:
        poll_list = json.load(json_file, parse_float=Decimal)
    load_polls(poll_list)

    with open("share/voters.json") as json_file:
        voter_list = json.load(json_file, parse_float=Decimal)
    load_voters(voter_list)

    with open("share/results.json") as json_file:
        result_list = json.load(json_file, parse_float=Decimal)
    load_results(result_list)
    add_secondary_index()
    print("All Databases Successfully created!\nClose the connection... (Ctrl+C)")
    # get_secondary_index_results("Should I go to Japan?")
