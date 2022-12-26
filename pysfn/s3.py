from typing import Union
from aws_cdk import (
    aws_stepfunctions_tasks as tasks,
    aws_s3 as s3,
)
from aws_cdk.aws_stepfunctions import JsonPath


def write_json(obj: Union[dict, list], bucket: s3.IBucket, key: str) -> str:
    pass


def read_json(bucket: s3.Bucket, key: str) -> (Union[dict, list], str, str):
    pass


def build_s3_write_json_step(
    stack,
    id_: str,
    obj: Union[dict, list, str],
    bucket: Union[str, s3.IBucket],
    key: Union[str, JsonPath],
):
    return tasks.CallAwsService(
        stack,
        id_,
        service="s3",
        action="putObject",
        iam_resources=["*"],
        input_path="$.register",
        result_path="$.register.out",
        result_selector={"ETag.$": "States.StringToJson($.ETag)"},
        parameters={
            "Bucket": JsonPath.string_at(bucket)
            if isinstance(bucket, str)
            else bucket.bucket_name,
            "Key": JsonPath.string_at(key) if not key.startswith("${") else key,
            "Body": JsonPath.string_at(obj),
            "ContentType": "application/json",
        },
    )


def build_s3_read_json_step(
    stack, id_: str, bucket: Union[str, s3.Bucket], key: Union[str, JsonPath],
):
    return tasks.CallAwsService(
        stack,
        id_,
        service="s3",
        action="getObject",
        iam_resources=["*"],
        input_path="$.register",
        result_path="$.register.out",
        result_selector={
            "Body.$": "States.StringToJson($.Body)",
            "LastModified.$": "$.LastModified",
            "ETag.$": "States.StringToJson($.ETag)",
        },
        parameters={
            "Bucket": JsonPath.string_at(bucket)
            if isinstance(bucket, str)
            else bucket.bucket_name,
            "Key": JsonPath.string_at(key) if not key.startswith("${") else key,
        },
    )
