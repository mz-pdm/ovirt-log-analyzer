"""Linking errors from logfiles to each other
This module contains methods of collecting errors' statistics
    
    For all logfiles:
    - merge_all_errors_by_time - returns timeline 
        (number of errors in each millisecond) and errors from all logfiles:
        time - logfile - [sender, thread, event, message]
    - calculate_frequency - calculates number of occurance of each \
        sender, event, message and generally number of errors in the logfile
"""
import numpy as np
import re
import matplotlib.pyplot as plt

def merge_all_errors_by_time(all_errors, fields_names):
    all_times = []
    set_headers = set([h for s in list(fields_names.values()) \
                                for h in s])
    set_headers.remove("date_time")
    set_headers.remove("line_num")
    set_headers.remove("message")
    list_headers = ["date_time", "line_num", "message"] + \
                                sorted(list(set_headers))
    for log in sorted(all_errors.keys()):
        for err in all_errors[log]:
            line = []
            for field_id, field in enumerate(list_headers):
                if field not in fields_names[log]:
                    line += ['']
                else:
                    #print('in this log', fields_names[log])
                    idx = fields_names[log].index(field)
                    #print('Index of', field, 'is', idx)
                    line += [err[idx]]
            all_times += [line]
    #all_times = sorted(all_times, key = lambda k: k[0])
    #min_time = all_times[0][0]
    #max_time = all_times[-1][0]
    #timeline = np.zeros((int(max_time) - int(min_time) + 1))
    #for error in all_times:
    #    timeline[int(error[0]) - int(min_time)] += 1
    timeline = []
    return timeline, all_times, list_headers

#many events in short time
#errors, warnings
#check 5-sec window
#tracebacks
#repeated actions
#error, down, warn
#many messages in the same millisecond
def calculate_errors_frequency(all_errors, timeline, fields):
    template = re.compile(\
        r"[^^]\"[^\"]{20,}\"|"+\
        r"[^^]\'[^\']{20,}\'|"+\
        r"[^^]\[+.*\]+|"+\
        r"[^^]\(+.*\)+|"+\
        r"[^^]\{+.*\}+|"+\
        r"[^^]\<+.*\>+|"+\
        r"[^^][^\ \t\,\;\=]{20,}|"+\
        r"[\d]+")
    #print(all_errors)
    #print(fields)
    msid = fields.index("message")
    for err in all_errors:
        mstext = err[msid]
        mstext = re.sub(template, '<...>', mstext)
        mstext = re.sub(re.compile(
            r"((\<\.\.\.\>[\ \.\,\:\;\{\}\(\)\[\]\$]*){2,})"), '<...>', mstext)
        mstext = re.sub(re.compile(
            r"(([\ \.\,\:\;\+\-\{\}]*"+\
            r"\<\.\.\.\>[\ \.\,\:\;\+\-\{\}]*)+)"), '<...>', mstext)
        #print(mstext)