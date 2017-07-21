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
def return_nonsimilar_part(str1, str2):
    str1_word = str1.split(' ')
    str2_word = str2.split(' ')
    set1 = set(str1_word)
    set2 = set(str2_word)
    diff1 = set1 - set2
    diff2 = set2 - set1
    return diff1.union(diff2)


def clusterize_messages(all_errors, fields, dirname):
    template = re.compile(\
    #    r"[^^][^\ \t\,\;\=]{20,}|" +
        r"[^^](\"[^\"]{20,}\")|" +
        r"[^^](\'[^\']{20,}\')|" +
        r"[^^](\(+.*\)+)|" +
        r"[^^](\[+.*\]+)|" +
        r"[^^](\{+.*\}+)|" +
        r"[^^](\<+.*\>+)")
# r"[\d]+")
    msid = fields.index("message")
    timeid = fields.index("date_time")
    events = {}
    for err_id in range(len(all_errors)):
        mstext = all_errors[err_id][msid]
        groups = re.findall(template, mstext)
        for g in groups:
            for subg in g:
                if subg == '':
                    continue
                mstext = mstext.replace(subg, '')
        all_errors[err_id] += [mstext]

        # for keyword in keywords:
        #     mstext = re.sub(keyword, '<...>', mstext)
        # mstext = re.sub(re.compile(
        #     r"((\<\.\.\.\>[\ \.\,\:\;\{\}\(\)\[\]\$]*){2,})"),
        #     '<...>', mstext)
        # mstext = re.sub(re.compile(
        #     r"(([\.\,\:\;\+\-\{\}]*" +
        #     r"\<\.\.\.\>[\.\,\:\;\+\-\{\}]*)+)"),
        #     '<...>', mstext)
        if (mstext not in events.keys()):
            events[mstext] = []
        events[mstext] += [[all_errors[err_id][msid],
                            all_errors[err_id][timeid]]]

    messages = sorted(events.keys())
    similar = np.zeros((len(messages), len(messages)))
    for err0 in range(len(messages)):
        for err1 in range(err0+1, len(messages)):
            msg1 = messages[err0].lower()
            msg2 = messages[err1].lower()
            dist = editdistance.eval(msg1, msg2)
            msg1 = re.sub('[:.,;]$', '', re.split(' |:', msg1)[0])
            msg2 = re.sub('[:.,;]$', '', re.split(' |:', msg2)[0])
            if msg1 == msg2:
                dist /= 5
            else:
                dist *= 10
            similar[err0, err1] = np.round(dist/len(messages[err0]), 1)
            similar[err1, err0] = np.round(dist/len(messages[err1]), 1)
    np.fill_diagonal(similar, 0)
    d = DBSCAN(metric='precomputed', min_samples=2)
    clust = d.fit_predict(similar)
    messages = sorted(zip(clust, messages), key=lambda k: k[0])
    # f = open("result_"+dirname.split('/')[-2]+".txt", 'w')
    # cur_mid = messages[0][0]
    # for mid in messages:
    #     if mid[0] != cur_mid:
    #         f.write('\n')
    #         cur_mid = mid[0]
    #     f.write("%d : %s\n" % (mid[0], mid[1]))
    # f.close()
    clusters = {}
    for clust, msg in messages:
        if clust not in clusters.keys():
            clusters[clust] = []
        clusters[clust] += [[msg] + events[msg]]
    return all_errors, fields + ['filtered'], clusters


# fields - names the following positions in log line (date_time,
# massage, field1, field2, etc.). It is list.
# all_errors [[msg, date_time, fields..., filtered], [],...]
# clusters {'1': [msg_filtered, mgs_orig, time],...}
def calculate_events_frequency(all_errors, clusters, fields, timeline, 
                               vms, hosts):
    f_msg_idx = fields.index('filtered')
    fields += ['cluster_num', 'cluster_len']
    for err_id in range(len(all_errors)):
        for c_id in sorted(clusters.keys(), key=lambda k: int(k)):
            if all_errors[err_id][f_msg_idx] in clusters[c_id]:
                all_errors[err_id] += [c_id]
                all_errors[err_id] += [len(clisters[c_id])]
    for c_id in sorted(clusters.keys(), key=lambda k: int(k)):
        if c_id == -1:
            continue
        diff = set()
        for msg1_id in range(len(clusters[c_id])-1):
            for msg2_id in range(msg1_id+1, len(clusters[c_id])):
                diff = diff.union(return_nonsimilar_part(clusters[c_id][msg1_id][0].lower(),
                                                clusters[c_id][msg2_id][0].lower()))
        #print(c_id,'>>>',diff)
    return all_errors
