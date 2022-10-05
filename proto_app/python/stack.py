import os
from time import sleep
from typing import List, Union
from aws_cdk import (
    aws_iam as iam,
    aws_lambda as lmbda,
    Duration,
    Stack,
)
from constructs import Construct
from pysfn.lmbda import PythonLambda, function_for_lambda
from pysfn.steps import state_machine, Retry
import operations


class ProtoAppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.lambda_role = iam.Role(
            self,
            "LambdaRole",
            role_name="LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambdaExecute")
            ],
        )

        base_lambda = PythonLambda(
            self,
            "pysfn-base-python",
            os.path.join(os.getcwd(), "python"),
            role=self.lambda_role,
            runtime=PythonLambda.PYTHON_3_9,
            timeout_minutes=1,
            memory_mb=1,
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
            memory_mb=10,
            # layers=[
            #    "arn:aws:lambda:us-east-1:999999999999:layer:Utilities:2",
            #    "arn:aws:lambda:us-east-1:999999999999:layer:Additional:1",
            # ],
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
            runtime=lmbda.Runtime.NODEJS_14_X,
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

        base_lambda.create_construct()
        high_memory_lambda.create_construct()

        @state_machine(self, "pysfn-simple", locals())
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

        @state_machine(self, "pysfn-basic", locals())
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

        # Not supported yet!!
        # @state_machine(self, "pysfn-larger", locals())
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
                return (
                    out_uri,
                    value_count,
                    valid,
                    has_detail,
                    score,
                    values,
                )
            except Exception:
                message = "Processing failed"
                return message
