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

def merge_all_errors_by_time(all_errors, fields_names):
    all_times = []
    all_loglinenumber = []
    for log in sorted(all_errors.keys()):
        time_index = fields_names[log].index('date_time')
        line_number_index = fields_names[log].index('line_number')
        for err in sorted(all_errors[log].keys()):
            all_times += [all_errors[log][err][time_index]]
            all_loglinenumber += [log+'_'+str(all_errors[log][err][line_number_index])]
    min_time = min(all_times)
    max_time = max(all_times)
    timeline = np.zeros((int(max_time//1000) - int(min_time//1000) + 1))
    
    all_errors = {}
    for log in sorted(all_errors.keys()):
        for dt_index, date_time in enumerate(all_times):
            timeline[int(date_time//1000) - int(min_time//1000)] += 1
            if not date_time in all_errors.keys():
                all_errors[date_time] = {}
            if log == all_loglinenumber[dt_index].split('_')[0]: 
                all_errors[date_time][log] += [int(all_loglinenumber[dt_index].split('_')[1])]
            else:
                all_errors[date_time][log] += [0]
    return timeline, all_errors