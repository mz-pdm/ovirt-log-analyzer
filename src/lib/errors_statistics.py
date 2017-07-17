""" Linking errors from logfiles to each other
This module contains methods of collecting errors' statistics
For all logfiles:
- merge_all_errors_by_time - returns timeline
    (number of errors in each millisecond) and errors from
    all logfiles:
    time - logfile - [sender, thread, event, message]
- calculate_frequency - calculates number of occurance of each
    sender, event, message and generally number of errors
    in the logfile
"""
import numpy as np
import re
import editdistance
# import json
from sklearn.cluster import DBSCAN


def merge_all_errors_by_time(all_errors, fields_names):
    all_times = []
    set_headers = set([h for s in list(fields_names.values())
                       for h in s])
    set_headers.remove("date_time")
    set_headers.remove("line_num")
    set_headers.remove("message")
    list_headers = ["date_time", "line_num", "message"] +\
        sorted(list(set_headers))
    for log in sorted(all_errors.keys()):
        for err in all_errors[log]:
            line = []
            for field_id, field in enumerate(list_headers):
                if field not in fields_names[log]:
                    line += ['']
                else:
                    # print('in this log', fields_names[log])
                    idx = fields_names[log].index(field)
                    # print('Index of', field, 'is', idx)
                    line += [err[idx]]
            all_times += [line]
    # all_times = sorted(all_times, key = lambda k: k[0])
    # min_time = all_times[0][0]
    # max_time = all_times[-1][0]
    # timeline = np.zeros((int(max_time) - int(min_time) + 1))
    # for error in all_times:
    #    timeline[int(error[0]) - int(min_time)] += 1
    timeline = []
    return timeline, all_times, list_headers


# many events in short time
# errors, warnings
# check 5-sec window
# tracebacks
# repeated actions
# error, down, warn
# many messages in the same millisecond
def return_nonsimilar_part(str1, str2, keywords):
    str1_word = str1.split(' ')
    str2_word = str2.split(' ')
    set1 = set(str1_word)
    set2 = set(str2_word)
    diff1 = set1 - set2
    diff2 = set2 - set1
    return diff1, diff2


def calculate_events_frequency(all_errors, keywords,
                               timeline, fields, dirname):
    template = re.compile(\
        # r"[^^][^\ \t\,\;\=]{20,}|" +
        # r"[^^]\"[^\"]{30,}\"|" +
        # r"[^^]\'[^\']{30,}\'|" +
        r"[^^]\(+.*\)+|" +
        r"[^^]\[+.*\]+|" +
        r"[^^]\{+.*\}+|" +
        r"[^^]\<+.*\>+")
# r"[\d]+")
    msid = fields.index("message")
    timeid = fields.index("date_time")
    events = {}
    for err_id in range(len(all_errors)):
        mstext = all_errors[err_id][msid]
        mstext = re.sub(template, '<...>', mstext)
        for keyword in keywords:
            mstext = re.sub(keyword, '<...>', mstext)
        mstext = re.sub(re.compile(
            r"((\<\.\.\.\>[\ \.\,\:\;\{\}\(\)\[\]\$]*){2,})"),
            '<...>', mstext)
        mstext = re.sub(re.compile(
            r"(([\ \.\,\:\;\+\-\{\}]*" +
            r"\<\.\.\.\>[\ \.\,\:\;\+\-\{\}]*)+)"),
            '<...>', mstext)
        if (mstext not in events.keys()):
            events[mstext] = []
        events[mstext] += [all_errors[err_id][timeid]]

    messages = sorted(events.keys())
    similar = np.zeros((len(messages), len(messages)))
    for err0 in range(len(messages)):
        for err1 in range(err0+1, len(messages)):
            dist = editdistance.eval(messages[err0].lower(),
                                     messages[err1].lower())
            similar[err0, err1] = np.round(dist/len(messages[err0]), 1)
            similar[err1, err0] = np.round(dist/len(messages[err1]), 1)
    np.fill_diagonal(similar, 0)

    d = DBSCAN(metric='precomputed', min_samples=2, eps=0.65)
    clust = d.fit_predict(similar)
    # print(clust)
    # print(list(clust).count(-1), 'of', len(list(clust)))
    f = open("result_"+dirname.split('/')[-2]+".txt", 'w')
    messages = sorted(zip(clust, messages), key=lambda k: k[0])
    cur_mid = messages[0][0]
    for mid in messages:
        if mid[0] != cur_mid:
            f.write('\n')
            cur_mid = mid[0]
        f.write("%d : %s\n" % (mid[0], mid[1]))
    f.close()
    # json.dump(events, f, indent=4, sort_keys=True)
