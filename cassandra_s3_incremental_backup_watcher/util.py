from __future__ import absolute_import, unicode_literals

import re


def clean_s3_path(path):
    path = re.sub(r'^/+', '', path)
    path = re.sub(r'/+$', '', path)
    return path
