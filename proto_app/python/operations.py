from typing import List


def step1(str_value: str, bool_value: bool) -> (bool, str, bool, int, int, str):
    return str_value in ["html", "image", "pdf"], str_value, False, 4, 200, "text/html"


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


def step10(uri: str):
    return ["one", "two", "three", "four", "five"]


def step11(val: str):
    return


def step12(val: str):
    return val.upper()
