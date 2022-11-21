import json
from typing import List
import time
from dataclasses import make_dataclass, dataclass
import boto3


@dataclass
class Step1Output:
    valid: bool
    str_value: str
    failed: bool
    int_value: int
    response_code: int
    content_type: str


def step1(str_value: str, bool_value: bool) -> (bool, str, bool, int, int, str):
    return str_value in ["html", "image", "pdf"], str_value, False, 4, 200, "text/html"


def step1_typed(str_value: str, bool_value: bool) -> Step1Output:
    return Step1Output(
        str_value in ["html", "image", "pdf"], str_value, False, 4, 200, "text/html",
    )


def step1_inline_typed(input: make_dataclass("Input", ["str_value", "bool_value"])):
    Output = make_dataclass(
        "Output",
        ["valid", "str_value", "failed", "int_value", "response_code", "content_type"],
    )
    return Output(
        input.str_value in ["html", "image", "pdf"],
        input.str_value,
        False,
        4,
        200,
        "text/html",
    )


def step2(str_value: str, list_value: List[int]):
    return True, [720, 520], "s3://mybucket/foo/XXXX.pdf"


def step3(str_value: str, str_value2: str, str_value3: str):
    if str_value2 == "image":
        return True, "s3://mybucket/foo/XXXX.png", None
    else:
        return True, None, "s3://mybucket/foo/XXXX.pdf"


def step4(str_value: str):
    return "s3://mybucket/foo/XXXX.png"


def step5(str_value: str, str_value2: str):
    return "s3://mybucket/foo/XXXX.json", True


def start_job(str_value: str, str_value2: str):
    return "XXXXXXXX"


def get_result(job_id, uri, raise_incomplete):
    return "s3://mybucket/foo/XXXX.json", True


def step6(str_value: str):
    return "s3://mybucket/foo/XXXX.json", True


def step7(str_value: str):
    return "s3://mybucket/foo/XXXX.png"


def step8(values: List):
    return values


def step9(values: List):
    return "s3://mybucket/foo/XXXX.json", 60, True, False, 0.8


def step10(uri: str, count: int):
    return ["one", "two", "three", "four", "five", "six", "seven", "eight", "nine"][
        :count
    ]


def step11(val: str):
    return


def step12(val: str):
    return val.upper()


def delayed_step(
    val: str,
    task_token: str = None,
    delay: int = 20,
    heartbeats: int = None,
    success: bool = True,
):
    sfn = boto3.client("stepfunctions")
    result = val.lower()
    if task_token:
        if heartbeats:
            for i in range(heartbeats):
                time.sleep(delay)
                print("Sending heartbeat")
                sfn.send_task_heartbeat(taskToken=task_token)
        time.sleep(delay)
        if success:
            print("Sending success")
            sfn.send_task_success(
                taskToken=task_token, output=json.dumps({"result": result})
            )
        else:
            print("Sending failure")
            sfn.send_task_failure(taskToken=task_token, error="Failed")
    else:
        time.sleep(delay)
        return result
