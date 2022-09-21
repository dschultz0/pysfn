import shortuuid


def shortid():
    return shortuuid.uuid()[:8]
