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
from datetime import datetime


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


def clusterize_messages(all_errors, fields, keywords, dirname):
    template = re.compile(\
                          # r"[^^][^\ \t\,\;\=]{20,}|" +
                          r"[^^](\"[^\"]{20,}\")|" +
                          r"[^^](\'[^\']{20,}\')|" +
                          r"[^^](\(+.*\)+)|" +
                          r"[^^](\[+.*\]+)|" +
                          r"[^^](\{+.*\}+)|" +
                          r"[^^](\<+.*\>+)")
    msid = fields.index("message")
    dtid = fields.index('date_time')
    fields += ['filtered']
    fid = fields.index('filtered')
    for err_id in range(len(all_errors)):
        mstext = all_errors[err_id][msid]
        groups = re.findall(template, mstext)
        for g in groups:
            for subg in g:
                if subg == '' or any([k in subg for k in keywords]):
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
    all_errors = [msg for msg in all_errors if len(msg[msid]) > 10]
    messages = [m[fid] for m in all_errors]
    messages2 = set()
    for mes in messages:
        lim = min(len(mes),70)
        messages2.add(mes.lower()[:lim])
    messages2 = sorted(list(messages2))
    similar = np.zeros((len(messages2), len(messages2)))
    regex1 = re.compile(r'[:.,;]$')
    # regex2 = re.compile(r' |:')
    for err0 in range(len(messages2)):
        print('clusterize_messages: %d from %d...'% (err0, len(messages2)))
        for err1 in range(err0+1, len(messages2)):
            dist = editdistance.eval(messages2[err0], messages2[err1])
            #msg1 = regex1.sub('', messages2[err0].split(' ')[0])
            #msg2 = regex1.sub('', messages2[err1].split(' ')[0])
            if messages2[err0].split(' ')[0] == messages2[err1].split(' ')[0]:
                similar[err0, err1] = (dist/5)//len(messages[err0])
                similar[err1, err0] = (dist/5)//len(messages[err1])
            else:
                similar[err0, err1] = (dist*10)//len(messages[err0])
                similar[err1, err0] = (dist*10)//len(messages[err1])
            # similar[err0, err1] = dist  # //len(messages[err0])
            # similar[err1, err0] = dist  # //len(messages[err1])
    np.fill_diagonal(similar, 0)
    similar += similar.T
    d = DBSCAN(metric='precomputed', min_samples=2)
    clust = d.fit_predict(similar)
    clust = [clust[id_f]
            for m_or in messages
            for id_f, m_f in enumerate(messages2)
            if (m_f == (m_or[:min(len(m_or),70)].lower()))]
    messages = sorted(zip(clust, messages, range(len(messages))),
                      key=lambda k: int(k[0]))
    f = open("result_"+dirname.split('/')[-2]+".txt", 'w')
    cur_mid = messages[0][0]
    for mid in messages:
        if mid[0] != cur_mid:
            f.write('\n')
            cur_mid = mid[0]
        f.write("%d : %s\n" % (mid[0], mid[1]))
    f.close()
    clusters = {}
    all_errors = [all_errors[i[2]] for i in messages]
    for (cluster_no, mes_f, k) in messages:
        if cluster_no not in clusters.keys():
            clusters[cluster_no] = []
        clusters[cluster_no] += [all_errors[k]]
    return clusters, all_errors, fields


# fields - names the following positions in log line (date_time,
# massage, field1, field2, etc.). It is list.
# all_errors [[msg, date_time, fields..., filtered], [],...]
# clusters {'1': [msg_filtered, mgs_orig, time],...}
def calculate_events_frequency(all_errors, clusters, fields, timeline,
                               keywords, vm_tasks, long_tasks, all_vms,
                               all_hosts):
    needed_msgs = set()
    reasons = {}
    msid = fields.index('message')
    dtid = fields.index('date_time')
    strid = fields.index('line_num')
    max_clust = max(sorted(clusters.keys(), key=lambda k: int(k)))
    for c_id in sorted(clusters.keys(), key=lambda k: int(k)):
        print('calculate_events_frequency: Cluster %d from %d...' %
                (c_id, max_clust))
        if c_id == -1:
            for msg in clusters[c_id]:
                # if len(msg[msid]) > 1000:
                #     continue
                # else:
                needed_msgs.add(msg[strid])
                if msg[strid] not in reasons.keys():
                    reasons[msg[strid]] = []
                reasons[msg[strid]] += ['Unique']
            continue
        if (len(clusters[c_id]) > len(all_errors)/20):
            continue
        for msg in clusters[c_id]:
            # Check if user-defined words are in the message
            for k in keywords:
                if k in msg[msid]:
                    needed_msgs.add(msg[strid])
                    if msg[strid] not in reasons.keys():
                        reasons[msg[strid]] = []
                    reasons[msg[strid]] += ['VM, Host or Task ID']
                    break
            # Check if long tasks are related to the message
            for com in sorted(long_tasks.keys()):
                for t in long_tasks[com]:
                    if ((com in msg[msid]) and
                            (t - 10 < msg[dtid] < t + 10)):
                        needed_msgs.add(msg[strid])
                        if msg[strid] not in reasons.keys():
                            reasons[msg[strid]] = []
                        reasons[msg[strid]] += ['Long operation']
                        break
            # Check if message is related to the VM commands
            for thread in (vm_tasks.keys()):
                for field in msg:
                    if thread in str(field):
                        needed_msgs.add(msg[strid])
                        if msg[strid] not in reasons.keys():
                            reasons[msg[strid]] = []
                        reasons[msg[strid]] += ['VM command']
        # endloop
        # Analyze the difference between messages in cluster
        print('calculate_events_frequency: Start diff...')
        diff = set()
        for msg1_id in range(len(clusters[c_id])-1):
            print('calculate_events_frequency: Msg %d from %d...' %
                  (msg1_id, len(clusters[c_id])))
            for msg2_id in range(msg1_id+1, len(clusters[c_id])):
                diff = diff.union(return_nonsimilar_part(
                            clusters[c_id][msg1_id][msid].lower(),
                            clusters[c_id][msg2_id][msid].lower()))
        if (sum([k in word for word in diff for k in all_vms]) >=
                len(all_vms)/2):
            # Show because includes VM or Host or task ID
            for msg in clusters[c_id]:
                needed_msgs.add(msg[strid])
                if msg[strid] not in reasons.keys():
                    reasons[msg[strid]] = []
                reasons[msg[strid]] += ['Differ by VM IDs']
        if len(set([len(w) for w in diff])) == 1:
            # Show because cluster differs by one-length words (usually IDs)
            if (np.std([msg[dtid] for msg in clusters[c_id]]) >=
                    len(clusters[c_id])/2):
                for msg in clusters[c_id]:
                    if msg[strid] in needed_msgs:
                        if msg[strid] not in reasons.keys():
                            reasons[msg[strid]] = []
                        needed_msgs.remove(msg[strid])
                        reasons[msg[strid]] += ['Repeats']
            else:
                for msg in clusters[c_id]:
                    needed_msgs.add(msg[strid])
                    if msg[strid] not in reasons.keys():
                        reasons[msg[strid]] = []
                    reasons[msg[strid]] += ['One-length IDs']
        else:
            # Show because includes keyword
            for msg in clusters[c_id]:
                for field in msg:
                    if any([k in str(field).lower() for k in
                            ['error ', 'fail', 'traceback',
                             'except', 'warn']]):
                        needed_msgs.add(msg[strid])
                        if msg[strid] not in reasons.keys():
                            reasons[msg[strid]] = []
                        reasons[msg[strid]] += ['Error or warning']
    # endloop
    for t in range(5, len(timeline)-5):
        if len(timeline[t-5:t])*2 < len(timeline[t:t+5]):
            # Show because an amount of followed messages increased
            for msg in timeline[t]:
                needed_msgs.add(msg[strid])
                if msg[strid] not in reasons.keys():
                    reasons[msg[strid]] = []
                reasons[msg[strid]] += ['Increased messages']
    # END ALGO
    msg_showed = []
    f = open('diff.txt', 'w')
    for idx, msg in enumerate(all_errors):
        if msg[strid] in needed_msgs:
            msg_showed += [[msg[dtid], msg[strid],
                            '_'.join(reasons[msg[strid]]), msg[msid]]]
        else:
            if msg[strid] in reasons.keys():
                reason = '_'.join(reasons[msg[strid]])
            else:
                reason = 'unknown'
            f.write("%12s %s | %10s | %15s | %s\n" %
                  (datetime.utcfromtimestamp(msg[dtid]).strftime(
                   "%H:%M:%S,%f")[:-3],
                   datetime.utcfromtimestamp(msg[dtid]).strftime(
                   "%d-%m-%Y"),
                   msg[strid], reason, msg[msid]))
    f.close()
    return msg_showed, ['date_time', 'line_num', 'reason', 'message']
