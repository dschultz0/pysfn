from typing import List


def step1(str_value: str, bool_value: bool) -> (bool, str, bool, int, int, str):
    return True, "html", False, 4, 200, "text/html"


def step2(str_value: str, list_value: List[int]):
    return True, [720, 520], "s3://mybucket/foo/XXXX.pdf"


def step3(str_value: str, str_value2: str, str_value3: str):
    return True, "s3://mybucket/foo/XXXX.png", "s3://mybucket/foo/XXXX.pdf"


def step4(str_value: str):
    return "s3://mybucket/foo/XXXX.png"


def step5(str_value: str, str_value2: str):
    return "s3://mybucket/foo/XXXX.json", 60


def start_job(str_value: str, str_value2: str):
    return "XXXXXXXX"


def get_result(job_id):
    return "s3://mybucket/foo/XXXX.json", 60


def step6(str_value: str):
    return "s3://mybucket/foo/XXXX.json", 60


def step7(str_value: str):
    return "s3://mybucket/foo/XXXX.png"


def step8(values: List):
    return values


def step9(values: List):
    return "s3://mybucket/foo/XXXX.json", 60, True, False, 0.8
