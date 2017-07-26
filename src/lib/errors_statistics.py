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
                    idx = fields_names[log].index(field)
                    line += [err[idx]]
            all_times += [line]
    all_times = sorted(all_times, key=lambda k: k[0])
    min_time = all_times[0][0]
    max_time = all_times[-1][0]
    timeline = []
    for t_id in range(int(max_time) - int(min_time) + 1):
        timeline += [[]]
    for error in all_times:
        timeline[int(error[0]) - int(min_time)] += [error]
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
                          # r"[^^][^\ \t\,\;\=]{20,}|" +
                          # r"[^^](\"[^\"]{20,}\")|" +
                          # r"[^^](\'[^\']{20,}\')|" +
                          r"[^^](\(+.*\)+)|" +
                          r"[^^](\[+.*\]+)|" +
                          r"[^^](\{+.*\}+)|" +
                          r"[^^](\<+.*\>+)")
    msid = fields.index("message")
    events = {}
    for err_id in range(len(all_errors)):
        mstext = all_errors[err_id][msid]
        groups = re.findall(template, mstext)
        for g in groups:
            for subg in g:
                if subg == '':
                    continue
                mstext = mstext.replace(subg, '')
        # all_errors[err_id] += [mstext]

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
        events[mstext] += [all_errors[err_id]]

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
    messages = sorted(zip(clust, messages), key=lambda k: int(k[0]))
    f = open("result_"+dirname.split('/')[-2]+".txt", 'w')
    cur_mid = messages[0][0]
    for mid in messages:
        if mid[0] != cur_mid:
            f.write('\n')
            cur_mid = mid[0]
        f.write("%d : %s\n" % (mid[0], mid[1]))
    f.close()
    clusters = {}
    for clust, msg in messages:
        if clust not in clusters.keys():
            clusters[clust] = []
        clusters[clust] += events[msg]
    return clusters


# fields - names the following positions in log line (date_time,
# massage, field1, field2, etc.). It is list.
# all_errors [[msg, date_time, fields..., filtered], [],...]
# clusters {'1': [msg_filtered, mgs_orig, time],...}
def calculate_events_frequency(clusters, fields, timeline, keywords):
    needed_msgs = []
    reasons = []
    msid = fields.index('message')
    dtid = fields.index('date_time')
    for c_id in sorted(clusters.keys(), key=lambda k: int(k)):
        if c_id == -1:
            for msg in clusters[c_id]:
                if len(msg[msid]) > 500:
                    continue
                needed_msgs += [msg]
                reasons += ['Unique']
            continue
        diff = set()
        for msg1_id in range(len(clusters[c_id])-1):
            for msg2_id in range(msg1_id+1, len(clusters[c_id])):
                diff = diff.union(return_nonsimilar_part(
                            clusters[c_id][msg1_id][msid].lower(),
                            clusters[c_id][msg2_id][msid].lower()))
        if sum([k in word for word in diff for k in keywords]) > 1:
            # Show because includes VM or Host or task ID
            added = False
            for msg in clusters[c_id]:
                if (msg[dtid] not in [m[dtid] for m in needed_msgs] or
                        msg[msid] not in [m[msid] for m in needed_msgs]):
                    needed_msgs += [msg]
                    reasons += ['VM, Host or Task ID']
                    added = True
                if added:
                    continue
        if any([k in (msg[msid]).lower() for msg in clusters[c_id] for
                k in ['error', 'down', 'fail', 'traceback', 'except',
                      'warning']]):
            # Show because includes keyword
            added = False
            for msg in clusters[c_id]:
                if (msg[dtid] not in [m[dtid] for m in needed_msgs] or
                        msg[msid] not in [m[msid] for m in needed_msgs]):
                    needed_msgs += [msg]
                    reasons += ['Keywords']
                    added = True
                if added:
                    continue
        if len(set([len(w) for w in diff])) == 1:
            # Show because cluster differs by one-length words (usually IDs)
            added = False
            for msg in clusters[c_id]:
                if (msg[dtid] not in [m[dtid] for m in needed_msgs] or
                        msg[msid] not in [m[msid] for m in needed_msgs]):
                    needed_msgs += [msg]
                    reasons += ['One-length IDs']
                    added = True
                if added:
                    continue
    for t in range(len(timeline)):
        if (t < 5 or t > len(timeline)-5):
            pass
        if len(timeline[t-5:t])*2 < len(timeline[t:t+5]):
            # Show because an amount of followed messages increased
            for m in range(len(timeline[t])):
                if ((timeline[t][m][dtid] not in [m[dtid]
                     for m in needed_msgs]) or
                    (timeline[t][m][msid] not in [m[msid]
                     for m in needed_msgs])):
                    needed_msgs += [timeline[t][m]]
                    reasons += ['Increased messages']
    sort_idxs = sorted(range(len(needed_msgs)),
                       key=lambda k: needed_msgs[k][dtid])
    sorted_messages = [needed_msgs[i] for i in sort_idxs]
    sorted_reasons = [reasons[i] for i in sort_idxs]
    return sorted_messages, sorted_reasons


def organize_important_tasks(all_tasks, long_tasks, all_errors):
    pass
