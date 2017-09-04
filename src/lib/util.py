import gzip
import lzma
import os


def open_log_file(file_name):
    if '.log' not in os.path.basename(file_name):
        return None
    mode = 'rt'
    if file_name.endswith('.xz'):
        f = lzma.open(file_name, mode)
    elif file_name.endswith('.gz'):
        f = gzip.open(file_name, mode)
    else:
        f = open(file_name, mode)
    return f
