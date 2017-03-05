import re
import os
import sys
import pytz
import heapq
import locale
from datetime import datetime
from dateutil.parser import parse
from tempfile import gettempdir
from itertools import islice, cycle
from collections import namedtuple

Keyed = namedtuple("Keyed", ["key", "obj"])
lastDate = parse("1970-01-01 00:00:00.00+0000")

def merge(key=None, *iterables):
    if key is None:
        keyed_iterables = iterables
    else:
        keyed_iterables = [(Keyed(key(obj), obj) for obj in iterable)
                            for iterable in iterables]
    for element in heapq.merge(*keyed_iterables):
        yield element.obj

def getKey(line):
    global lastDate
    reg = re.compile('^[ 0-9:+\.\-TZ,.]{19,28}')
    date = reg.search(line)
    try:
        if date is not None:
            parsed = parse(date.group(0))
            parsed.replace(tzinfo=pytz.UTC)
            if (parse(date.group(0)).tzinfo == None):
                parsed = parsed.replace(tzinfo=pytz.UTC)
            lastDate = parsed
            return parsed
        else:
            return lastDate
    except ValueError as v:
        print str(v) + " " + line

        return parse("1970-01-01 00:00:00.00+0000")

def batch_sort(input, output, key=None, buffer_size=32000, tempdirs=None):
    if tempdirs is None:
        tempdirs = []
    if not tempdirs:
        tempdirs.append(gettempdir())

    chunks = []
    try:
        with open(input,'rb',64*1024) as input_file:

            input_iterator = iter(input_file)
            for tempdir in cycle(tempdirs):
                current_chunk = list(islice(input_iterator,buffer_size))
                if not current_chunk:
                    break
                current_chunk.sort(key=lambda v: getKey(v))
                output_chunk = open(os.path.join(tempdir,'%06i'%len(chunks)),'w+b',64*1024)
                chunks.append(output_chunk)
                output_chunk.writelines(current_chunk)
                output_chunk.flush()
                output_chunk.seek(0)
        with open(output,'wb',64*1024) as output_file:
            output_file.writelines(merge(lambda v: getKey(v), *chunks))
    finally:
        for chunk in chunks:
            try:
                chunk.close()
                os.remove(chunk.name)
            except Exception:
                pass

files = sys.argv[1:]

if len(files) < 1:
    print "Error : Please enter filename"

with open('merge.txt', 'w') as outfile:
    for fname in files:
        with open(fname) as infile:
            fname = os.path.basename(infile.name)
            for line in infile:
                line = line.rstrip('\n') + "\t" + "(" + fname + ")" + '\n'
                outfile.write(line)

locale.setlocale(locale.LC_ALL,'C.UTF-8')

batch_sort('merge.txt','merger.txt')
