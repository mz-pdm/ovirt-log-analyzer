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
        for err_id in range(len(all_errors[log])):
            all_errors[log][err_id][1] = log+':'+str(all_errors[log][err_id][1])
            all_times += [all_errors[log][err_id]]
    all_times = sorted(all_times, key = lambda k: k[0])
    min_time = all_times[0][0]
    max_time = all_times[-1][0]
    timeline = np.zeros((int(max_time) - int(min_time) + 1))
    for error in all_times:
        timeline[int(error[0]) - int(min_time)] += 1
    return timeline, all_times