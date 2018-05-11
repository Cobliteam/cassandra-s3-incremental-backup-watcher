import re


def clean_s3_path(path):
    path = re.sub(r'^/+', '', path)
    path = re.sub(r'/+$', '', path)
    return path
