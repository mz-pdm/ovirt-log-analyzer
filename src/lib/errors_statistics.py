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
import matplotlib.pyplot as plt

def merge_all_errors_by_time(all_errors, fields_names):
    all_times = []
    for log in sorted(all_errors.keys()):
        all_times += all_errors[log]
    all_times = sorted(all_times, key = lambda k: k[0])
    min_time = all_times[0][0]
    max_time = all_times[-1][0]
    timeline = np.zeros((int(max_time) - int(min_time) + 1))
    for error in all_times:
        timeline[int(error[0]) - int(min_time)] += 1
    return timeline, all_times

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
    msid = fields.index("message")
    for err in all_errors:
        mstext = err[msid]
        mstext = re.sub(template, '<...>', mstext)
        mstext = re.sub(re.compile(
            r"((\<\.\.\.\>[\ \.\,\:\;\{\}\(\)\[\]\$]*){2,})"), '<...>', mstext)
        mstext = re.sub(re.compile(
            r"(([\ \.\,\:\;\+\-\{\}]*"+\
            r"\<\.\.\.\>[\ \.\,\:\;\+\-\{\}]*)+)"), '<...>', mstext)
        print(mstext)