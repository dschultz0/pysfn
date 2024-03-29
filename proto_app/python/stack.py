import os
import typing
from time import sleep
from typing import List, Union
from dataclasses import dataclass
from aws_cdk import (
    aws_iam as iam,
    aws_lambda as lmbda,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    Duration,
    Stack,
)
from constructs import Construct
from pysfn.lmbda import PythonLambda, function_for_lambda
from pysfn.service_operations import (
    s3_write_json,
    s3_read_json,
    sqs_send_message,
    dynamo_write_item,
    dynamo_read_item,
    dynamo_update_item,
    sqs_receive_message,
    sqs_delete_message,
)
from pysfn.steps import (
    state_machine,
    Retry,
    concurrent,
    event,
    await_token,
    execution_start_time,
    state_entered_time,
    ExecutionContext,
)
from . import operations


@dataclass
class Result:
    available: bool = None
    list_value: List[int] = None
    uri: str = None
    message: str = None


class ProtoAppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket = s3.Bucket(self, "pysfn-bucket")
        self.table = dynamodb.Table(
            self,
            "pysfn-table",
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            partition_key=dynamodb.Attribute(
                name="id", type=dynamodb.AttributeType.STRING
            ),
        )
        self.queue = sqs.Queue(self, "pysfn-queue")

        self.lambda_role = iam.Role(
            self,
            "LambdaRole",
            role_name="pysfn-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambdaExecute")
            ],
            inline_policies={
                "send-sfn-token": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "states:SendTaskSuccess",
                                "states:SendTaskFailure",
                                "states:SendTaskHeartbeat",
                            ],
                            effect=iam.Effect.ALLOW,
                            resources=["*"],
                        ),
                    ]
                )
            },
        )

        base_lambda = PythonLambda(
            self,
            "pysfn-base-python",
            os.path.join(os.getcwd(), "python"),
            role=self.lambda_role,
            runtime=PythonLambda.PYTHON_3_9,
            timeout_minutes=5,
            memory_gb=1,
            # layers=["arn:aws:lambda:us-east-1:999999999999:layer:Utilities:2"],
            environment=None,
        )
        high_memory_lambda = PythonLambda(
            self,
            "pysfn-highmemory-python",
            os.path.join(os.getcwd(), "python"),
            role=self.lambda_role,
            runtime=PythonLambda.PYTHON_3_9,
            timeout_minutes=15,
            memory_gb=10,
            environment={"NLTK_DATA": "/opt/nltk"},
        )

        js_lambda = lmbda.Function(
            self,
            "JSLambda",
            function_name="pysfn-js",
            code=lmbda.Code.from_asset(
                os.path.join(os.getcwd(), "js"), exclude=["node_modules"]
            ),
            handler="app.handler",
            runtime=lmbda.Runtime.NODEJS_18_X,
            role=self.lambda_role,
            timeout=Duration.minutes(10),
            memory_size=2096,
        )

        # Lambdas for the Basic SFN
        step1 = base_lambda.register(operations.step1)
        step2 = function_for_lambda(
            js_lambda,
            {"strValue": str, "optParam": bool},
            {"available": bool, "listValue": List[int], "resultURI": str},
        )
        step3 = high_memory_lambda.register(operations.step3)
        step4 = base_lambda.register(operations.step4)

        # Lambdas for the Larger SFN
        step5 = base_lambda.register(operations.step5)
        start_job = base_lambda.register(operations.start_job)
        get_result = base_lambda.register(operations.get_result)
        step6 = high_memory_lambda.register(operations.step6)
        step7 = high_memory_lambda.register(operations.step7)
        step8 = base_lambda.register(operations.step8)
        step9 = base_lambda.register(operations.step9)
        step10 = base_lambda.register(operations.step10)
        step11 = base_lambda.register(operations.step11)
        step12 = base_lambda.register(operations.step12)
        delayed_step = base_lambda.register(operations.delayed_step)

        base_lambda.create_construct()
        high_memory_lambda.create_construct()

        @state_machine(self, "pysfn-simple")
        def simple(str_value: str, list_value: List[int] = None, option: bool = False):
            uri1: Union[str, None] = None
            uri2: Union[str, None] = None
            (
                available,
                mode,
                option,
                processing_seconds,
                code_value,
                type_value,
            ) = step1(str_value, option)

            if available:
                (available, list_value, uri1) = step2(str_value, list_value)
            return (
                mode,
                code_value,
                processing_seconds,
                available,
                uri1,
                uri2,
                option,
            )

        @state_machine(self, "pysfn-basic")
        def basic(str_value: str, list_value: List[int] = None, option: bool = False):
            uri1: Union[str, None] = None
            uri2: Union[str, None] = None
            (
                available,
                mode,
                option,
                processing_seconds,
                code_value,
                type_value,
            ) = step1(str_value, option)

            if available:
                if mode == "html":
                    (available, list_value, uri1) = step2(str_value, list_value)
                else:
                    (available, uri2, uri1) = step3(str_value, mode, code_value)
                if uri1:
                    uri2 = step4(uri1)
            return (
                mode,
                code_value,
                processing_seconds,
                available,
                uri1,
                uri2,
                option,
            )

        @state_machine(self, "pysfn-larger")
        def larger(uri1: str, uri2: Union[str, None] = None):
            out_uri1: Union[str, None] = None
            out_uri4: Union[str, None] = None
            out_uri5: Union[str, None] = None
            if uri2:
                (out_uri1, value_count) = step5(uri2, uri1)
            job_id = start_job(uri1, uri2)
            sleep(10)
            with Retry(
                ["States.TaskFailed"],
                interval_seconds=10,
                backoff_rate=1.2,
                max_attempts=40,
            ):
                out_uri2, value_count = get_result(job_id, uri1, True)
            out_uri3, value_count = step6(uri1)
            try:
                alt_uri = step7(uri1)
                out_uri4, value_count = step6(alt_uri)
            except Exception:
                pass
            try:
                alt_uri = step7(uri1, True)
                out_uri5, value_count = step6(alt_uri)
            except Exception:
                pass
            try:
                values = step8([out_uri1, out_uri2, out_uri3, out_uri4, out_uri5,])
                (out_uri, value_count, valid, has_detail, score,) = step9(values)
                return out_uri, value_count, valid, has_detail, score, values, None
            except Exception:
                message = "Processing failed"
                return None, None, None, None, None, None, message

        @state_machine(self, "pysfn-larger-variant")
        def larger_variant(uri1: str, uri2: Union[str, None] = None):
            successful_uris: List[str] = []
            failed_uris: List[str] = []
            successful_count: int = 0
            failed_count: int = 0
            if uri2:
                result_uri, success = step5(uri2, uri1)
                if success:
                    successful_uris.append(result_uri)
                    successful_count += 1
                else:
                    failed_uris.append(result_uri)
                    failed_count += 1
            job_id = start_job(uri1, uri2)
            sleep(10)
            with Retry(
                ["States.TaskFailed"],
                interval_seconds=10,
                backoff_rate=1.2,
                max_attempts=40,
            ):
                result_uri, success = get_result(job_id, uri1, True)
                if success:
                    successful_uris.append(result_uri)
                    successful_count += 1
                else:
                    failed_uris.append(result_uri)
                    failed_count += 1
            result_uri, success = step6(uri1)
            try:
                alt_uri = step7(uri1)
                result_uri, success = step6(alt_uri)
                if success:
                    successful_uris.append(result_uri)
                    successful_count += 1
                else:
                    failed_uris.append(result_uri)
                    failed_count += 1
            except Exception:
                pass
            try:
                alt_uri = step7(uri1, True)
                result_uri, success = step6(alt_uri)
                if success:
                    successful_uris.append(result_uri)
                    successful_count += 1
                else:
                    failed_uris.append(result_uri)
                    failed_count += 1
            except Exception:
                pass
            try:
                values = step8(successful_uris)
                (out_uri, value_count, valid, has_detail, score,) = step9(values)
                return (
                    out_uri,
                    value_count,
                    valid,
                    has_detail,
                    score,
                    values,
                    None,
                    len(successful_uris),
                )
            except Exception:
                message = "Processing failed"
                return None, None, None, None, None, None, message, len(successful_uris)

        @state_machine(self, "pysfn-mapping")
        def mapping(uri: str, count: int = 5):
            values = step10(uri, count)
            r = range(10)
            results = []
            results2 = []
            r_vals = []
            for val in concurrent(values, 3):
                step11(val)
                res = step12(val)
                results.append(res)
            for val in concurrent(step10(uri, count), 3):
                step11(val)
                res = step12(val)
                results2.append(res)
            for val in range(5):
                r_vals.append(val)
            for val in range(3):
                step11(val)
            # results2 = [step12(v) for v in values]
            return results, results2, r_vals

        @state_machine(self, "pysfn-batch")
        def batch(uris: List[str]):
            for uri in uris:
                larger_variant(
                    uri,
                    sfn_execution_name=sfn.JsonPath.format(
                        "{}-{}",
                        sfn.JsonPath.string_at(ExecutionContext.name),
                        sfn.JsonPath.array_get_item(
                            sfn.JsonPath.string_split(sfn.JsonPath.uuid(), "-"), 1
                        ),
                    ),
                )

        """
        """

        @state_machine(self, "pysfn-callback")
        def token_callback(str_value: str):
            # Simply trigger asynchronously
            event(delayed_step(str_value))
            error = None

            # Callback will be called after 30 seconds, sfn will wait up to 2 minutes
            result = await_token(
                delayed_step(str_value, sfn.JsonPath.task_token, 30),
                ["result"],
                Duration.minutes(2),
            )

            # Heartbeat every 20 seconds, callback after 5, sfn will wait 30 seconds between heartbeats
            result = await_token(
                delayed_step(str_value, sfn.JsonPath.task_token, 20, 5),
                ["result"],
                Duration.seconds(30),
            )

            # Callback will be called after 30 seconds, sfn will only wait 20 seconds
            try:
                result = await_token(
                    delayed_step(str_value, sfn.JsonPath.task_token, 30),
                    ["result"],
                    Duration.seconds(20),
                )
            except Exception as ex:
                error = ex

            # Callback will be called after 20 seconds with a failure
            try:
                result = await_token(
                    delayed_step(str_value, sfn.JsonPath.task_token, 20, success=False),
                    ["result"],
                    Duration.minutes(2),
                )
            except Exception as ex:
                error = ex

            return result, error

        @state_machine(self, "pysfn-s3")
        def s3_read_write(str_value: str, option: bool = False):
            (
                available,
                mode,
                option,
                processing_seconds,
                code_value,
                type_value,
            ) = step1(str_value, option)
            obj = {
                "available": available,
                "mode": mode,
                "type_value": type_value,
                "execution_time": execution_start_time(),
                "state_time": state_entered_time(),
            }
            key = sfn.JsonPath.format("{}.json", sfn.JsonPath.uuid())
            etag = s3_write_json(obj, self.bucket, key)
            read_obj, last_modified, read_etag = s3_read_json(self.bucket, key)

        @state_machine(self, "pysfn-sqs")
        def sqs_send_receive(message: typing.Union[str, dict]):
            message_id = sqs_send_message(self.queue, message)
            sleep(5)
            messages = sqs_receive_message(self.queue, wait_time_seconds=10)
            for message in messages:
                sqs_delete_message(self.queue, message["ReceiptHandle"])

        # @state_machine(self, "pysfn-dynamo")
        def dynamo_read_write(str_value: str, option: bool = False):
            (
                available,
                mode,
                option,
                processing_seconds,
                code_value,
                type_value,
            ) = step1(str_value, option)
            item = {
                "id": sfn.JsonPath.uuid(),
                "available": available,
                "mode": mode,
                "type_value": type_value,
                "execution_time": execution_start_time(),
                "state_time": state_entered_time(),
            }
            dynamo_write_item(self.table, item)
            key = {"id": item["id"]}
            dynamo_read_item(self.table, key)

        @state_machine(self, "pysfn-dataclass")
        def step_dataclass(str_value: str, list_value: List[int] = None):
            result = Result(*step2(str_value, list_value))
            result2 = Result(message="Woah")
            result.message = "success"
            return result

        def sub_step(str_value: str, option: bool = False):
            (
                available,
                mode,
                option,
                processing_seconds,
                code_value,
                type_value,
            ) = step1(str_value, option)
            return available

        # @state_machine(self, "pysfn-simple-sub-step")
        def simple2(str_value: str, list_value: List[int] = None, option: bool = False):
            uri1: Union[str, None] = None
            uri2: Union[str, None] = None
            available = sub_step(str_value, option)
            if available:
                (available, list_value, uri1) = step2(str_value, list_value)
            return (
                available,
                uri1,
                uri2,
                option,
            )
