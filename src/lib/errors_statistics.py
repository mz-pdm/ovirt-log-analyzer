""" Linking errors from logfiles to each other
"""
import numpy as np
import re
from datetime import datetime


def merge_all_errors_by_time(all_errors, fields_names):
    all_messages = []
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
            for field in list_headers:
                if field not in fields_names[log]:
                    line += ['']
                else:
                    idx = fields_names[log].index(field)
                    line += [err[idx]]
            all_messages += [line]
    all_messages = sorted(all_messages, key=lambda k: k[0])
    min_time = all_messages[0][0]
    max_time = all_messages[-1][0]
    timeline = []
    for t_id in range(int(max_time) - int(min_time) + 1):
        timeline += [[]]
    for error in all_messages:
        if any([w in str(f).lower() for f in error for w in
                ['error', 'traceback', 'warn', 'fail']]):
            timeline[int(error[0]) - int(min_time)] += [error]
    return timeline, all_messages, list_headers


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
    strid = fields.index('line_num')
    fields += ['filtered']
    fid = fields.index('filtered')
    events = {}
    reasons = {}
    needed_msgs = set()
    all_errors = [msg for msg in all_errors if len(msg[msid]) > 10]
    for err_id in range(len(all_errors)):
        if err_id % 100 == 0:
            print(('clusterize_messages: Preprocessing %s from %s') %
                  (err_id, len(all_errors)), end='\r')
        mstext = all_errors[err_id][msid]
        groups = re.findall(template, mstext)
        for g in groups:
            for subg in g:
                if subg == '' or any([k in subg for k in keywords]):
                    continue
                mstext = mstext.replace(subg, '')
        mstext = mstext[:min(80, len(mstext))]
        if mstext not in events.keys():
            events[mstext] = {'date_time': [], 'line_num': [],
                              'keywords': set()}
        for k in keywords:
            if k in all_errors[err_id][msid]:
                events[mstext]['keywords'].add(k)
        for k in ['error', 'fail', 'failure', 'failed', 'traceback',
                  'warn', 'warning', 'could not', 'exception', 'down',
                  'crash']:
            for f in fields:
                err_res = re.search(r'(^|[ \:\.\,]+)' + k + r'([ \:\.\,=]+|$)',
                                    all_errors[err_id][msid].lower())
                if err_res is not None:
                    #  events[mstext]['keywords'].add(k)
                    if (k == 'traceback' or f != 'message'):
                        needed_msgs.add(all_errors[err_id][strid])
                        if all_errors[err_id][strid] not in reasons.keys():
                            reasons[all_errors[err_id][strid]] = set()
                        reasons[all_errors[err_id][strid]].add(
                            'Error or Traceback')
        events[mstext]['date_time'] += [all_errors[err_id][dtid]]
        events[mstext]['line_num'] += [all_errors[err_id][strid]]
        all_errors[err_id] += [mstext]
    print()
    mean_len = np.mean([len(events[g]['line_num']) for g in events.keys()])
    std_len = np.std([len(events[g]['line_num']) for g in events.keys()])
    for filtered in events.keys():
        if len(events[filtered]['line_num']) > mean_len + 3*std_len:
            for line_num in events[filtered]['line_num']:
                if line_num not in reasons.keys():
                    reasons[line_num] = set()
                reasons[line_num].add('Many messages')
        if (len(events[filtered]['keywords']) > 1):
            # Differ by VMiD
            for line_num in events[filtered]['line_num']:
                needed_msgs.add(line_num)
                if line_num not in reasons.keys():
                    reasons[line_num] = set()
                reasons[line_num].add('Differ by VM IDs')
    messages = [m[fid] for m in all_errors]  # if m[fid] in needed_msgs]
    messages2 = set()
    for mes in messages:
        # lim = min(len(mes),70)
        messages2.add(mes.lower())  # [:lim])
    messages2 = sorted(list(messages2))
    regex2 = re.compile(r'^(.{2,}?)([ =+-\:\;\,\'\"].*|$)')
    similar = {}
    for err0 in range(len(messages2)):
        shorten = regex2.search(messages2[err0]).group(1)
        if shorten not in similar.keys():
            similar[shorten] = []
        similar[shorten] += [messages2[err0]]

    f = open("result_"+dirname.split('/')[-2]+".txt", 'w')
    clust = sorted(similar.keys())
    for c_id, mid in enumerate(clust):
        for mes in similar[mid]:
            f.write("%d : %s\n" % (c_id, mes))
        f.write('\n')
    f.close()
    clusters = {}
    for err_id in range(len(all_errors)):
        for idx, cluster_name in enumerate(clust):
            for mes in similar[cluster_name]:
                if (all_errors[err_id][fid].lower() == mes):
                    # [:min(len(all_errors[err_id][fid]),70)].lower() == mes):
                    if idx not in clusters.keys():
                        clusters[idx] = []
                    clusters[idx] += [all_errors[err_id]]
    return clusters, all_errors, fields, needed_msgs, reasons


# fields - names the following positions in log line (date_time,
# massage, field1, field2, etc.). It is list.
# all_errors [[msg, date_time, fields..., filtered], [],...]
# clusters {'1': [msg_filtered, mgs_orig, time],...}
def calculate_events_frequency(all_errors, clusters, fields, err_timeline,
                               keywords, vm_tasks, long_tasks, all_vms,
                               all_hosts, needed_msgs, reasons):
    msid = fields.index('message')
    dtid = fields.index('date_time')
    strid = fields.index('line_num')
    fid = fields.index('filtered')
    max_clust = max(clusters.keys())
    mean_clust_len = np.mean([len(clusters[c]) for c in set(clusters.keys())])
    for c_id in sorted(clusters.keys(), key=lambda k: int(k)):
        print(("calculate_events_frequency: Cluster %s from %s") %
              (c_id, max_clust), end='\r')
        if ((len(clusters[c_id]) > 2*mean_clust_len) and not
                any([k in clusters[c_id][0][fid] for k in keywords])):
            for msg in clusters[c_id]:
                if msg[strid] not in reasons.keys():
                    reasons[msg[strid]] = set()
                if (msg[strid] in needed_msgs
                        and len(reasons[msg[strid]]) == 0
                        or reasons[msg[strid]] == 'Many messages'):
                    needed_msgs.remove(msg[strid])
                reasons[msg[strid]].add('Frequent')
            continue
        if (len(clusters[c_id]) == 1):
            needed_msgs.add(clusters[c_id][0][strid])
            if clusters[c_id][0][strid] not in reasons.keys():
                reasons[clusters[c_id][0][strid]] = set()
            reasons[clusters[c_id][0][strid]].add('Unique')
        for msg in clusters[c_id]:
            # Check if user-defined words are in the message
            for k in keywords:
                if k in msg[msid]:
                    needed_msgs.add(msg[strid])
                    if msg[strid] not in reasons.keys():
                        reasons[msg[strid]] = set()
                    reasons[msg[strid]].add('VM, Host or Task ID')
                    break
            # Check if long tasks are related to the message
            for com in sorted(long_tasks.keys()):
                added = False
                for t in long_tasks[com]:
                    if ((com in msg[msid]) and
                            (t - 10 < msg[dtid] < t + 10)):
                        needed_msgs.add(msg[strid])
                        if msg[strid] not in reasons.keys():
                            reasons[msg[strid]] = set()
                        reasons[msg[strid]].add('Long operation')
                        added = True
                        break
                if added:
                    break
            # Check if message is related to the VM commands
            for field in msg:
                added = False
                # Show because includes keyword
                # if any([k in str(field).lower() for k in
                #        ['error ', 'fail', 'traceback',
                #         'except', 'warn']]):
                #    needed_msgs.add(msg[strid])
                #    if msg[strid] not in reasons.keys():
                #        reasons[msg[strid]] = set()
                #    reasons[msg[strid]].add('Error or warning')
                #    added = True
                for thread in (vm_tasks.keys()):
                    for task in vm_tasks[thread]:
                        if task['command_name'] in str(field):
                            needed_msgs.add(msg[strid])
                            if msg[strid] not in reasons.keys():
                                reasons[msg[strid]] = set()
                            reasons[msg[strid]].add('VM command')
                            added = True
                            break
                if added:
                    break
        # endloop
    # endloop
    print()
    for t in range(10, len(err_timeline)-10):
        if len(err_timeline[t-10:t]) < len(err_timeline[t:t+10]):
            # Show because an amount of followed messages increased
            for msg in err_timeline[t]:
                needed_msgs.add(msg[strid])
                if msg[strid] not in reasons.keys():
                    reasons[msg[strid]] = set()
                reasons[msg[strid]].add('Increased errors')
    # END ALGO
    msg_showed = []
    new_fields = ['date_time', 'line_num', 'reason', 'message']
    f = open('diff.txt', 'w')
    max_len = max([len('_'.join(reasons[r])) for r in reasons.keys()])
    for msg in all_errors:
        if msg[strid] in needed_msgs:
            msg_showed += [[msg[dtid], msg[strid],
                            '_'.join(sorted(reasons[msg[strid]])), msg[msid]]]
        else:
            if msg[strid] in reasons.keys():
                reason = '_'.join(sorted(reasons[msg[strid]]))
            else:
                reason = 'unknown'
            f.write("%12s %s | %20s | %*s | %s\n" %
                    (datetime.utcfromtimestamp(msg[dtid]).strftime(
                                                        "%H:%M:%S,%f")[:-3],
                     datetime.utcfromtimestamp(msg[dtid]).strftime(
                                                        "%d-%m-%Y"),
                     msg[strid], max_len, reason, msg[msid]))
    f.close()
    msg_showed = sorted(msg_showed, key=lambda k: k[0])
    prev_message = msg_showed[0][3]
    for msg in (msg_showed[1:]).copy():
        if msg[3] == prev_message:
            msg_showed.remove(msg)
        prev_message = msg[3]
    return msg_showed, new_fields
