from typing import Union
from aws_cdk import (
    aws_stepfunctions_tasks as tasks,
    aws_dynamodb as ddb,
)
from aws_cdk.aws_stepfunctions import JsonPath


def write_item(table: ddb.Table, item: dict):
    pass


def read_item(table: ddb.Table, key: dict):
    pass


def delete_item(table: ddb.Table, key: dict):
    pass


def update_item(table: ddb.Table, key: dict, attribute_updates: dict):
    pass
