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
import progressbar


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
        if any([w in str(f).lower() for f in error for w in
                ['error', 'traceback', 'warning', 'fail']]):
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
    all_errors = [msg for msg in all_errors if len(msg[msid]) > 10]
    for err_id in range(len(all_errors)):
        mstext = all_errors[err_id][msid]
        groups = re.findall(template, mstext)
        for g in groups:
            for subg in g:
                if subg == '' or any([k in subg for k in keywords]):
                    continue
                mstext = mstext.replace(subg, '')
        all_errors[err_id] += [mstext]
    messages = [m[fid] for m in all_errors]
    messages2 = set()
    for mes in messages:
        lim = min(len(mes),70)
        messages2.add(mes.lower()[:lim])
    messages2 = sorted(list(messages2))
    similar = np.zeros((len(messages2), len(messages2)))
    regex1 = re.compile(r'[:.,;]$')
    # widget_style = ['All: ', progressbar.Percentage(), ' (',
    #                         progressbar.SimpleProgress(), ')', ' ',
    #                         progressbar.Bar(), ' ', progressbar.Timer(), ' ',
    #                         progressbar.AdaptiveETA()]
    #bar = ProgressBar(widgets=widget_style)
    #with Pool(processes=5) as pool:
    #    worker = pool.imap(star, run_args)
    #    for _ in bar(run_args):
    #        result += [worker.next()]
    for err0 in range(len(messages2)):
        print('clusterize_messages: %d from %d...'% (err0, len(messages2)))
        for err1 in range(err0+1, len(messages2)):
            dist = editdistance.eval(messages2[err0], messages2[err1])
            if messages2[err0].split(' ')[0] == messages2[err1].split(' ')[0]:
                similar[err0, err1] = dist//len(messages[err0])
                similar[err1, err0] = dist//len(messages[err1])
            else:
                similar[err0, err1] = (dist*100)//len(messages[err0])
                similar[err1, err0] = (dist*100)//len(messages[err1])
    np.fill_diagonal(similar, 0)
    similar += similar.T
    d = DBSCAN(metric='precomputed', min_samples=1)
    clust = d.fit_predict(similar)
    print('clusterize_messages: clusters created')
    f = open("result_"+dirname.split('/')[-2]+".txt", 'w')
    cur_mid = 0
    print('clusterize_messages: file opened')
    for mid in sorted(zip(clust, messages2), key=lambda k: int(k[0])):
        if mid[0] != cur_mid:
            f.write('\n')
            cur_mid = mid[0]
        f.write("%d : %s\n" % (mid[0], mid[1]))
    f.close()
    print('clusterize_messages: file writed')
    clusters = {}
    # slow place
    for err_id in range(len(all_errors)):
        for idx, cluster_no in enumerate(clust):
            if (all_errors[err_id][fid][:min(
                    len(all_errors[err_id][fid]),70)].lower() ==
                    messages2[idx]):
                if cluster_no not in clusters.keys():
                    clusters[cluster_no] = []
                clusters[cluster_no] += [all_errors[err_id]]
    print('clusterize_messages: end')
    return clusters, all_errors, fields


# fields - names the following positions in log line (date_time,
# massage, field1, field2, etc.). It is list.
# all_errors [[msg, date_time, fields..., filtered], [],...]
# clusters {'1': [msg_filtered, mgs_orig, time],...}
def calculate_events_frequency(all_errors, clusters, fields, err_timeline,
                               keywords, vm_tasks, long_tasks, all_vms,
                               all_hosts):
    needed_msgs = set()
    reasons = {}
    msid = fields.index('message')
    dtid = fields.index('date_time')
    strid = fields.index('line_num')
    max_clust = max(clusters.keys())
    mean_clust_len = np.mean([len(clusters[c]) for c in set(clusters.keys())])
    for c_id in sorted(clusters.keys(), key=lambda k: int(k)):
        print('calculate_events_frequency: Cluster %d from %d...' %
                (c_id, max_clust))
        if (len(clusters[c_id]) > 2*mean_clust_len):
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
                for t in long_tasks[com]:
                    if ((com in msg[msid]) and
                            (t - 10 < msg[dtid] < t + 10)):
                        needed_msgs.add(msg[strid])
                        if msg[strid] not in reasons.keys():
                            reasons[msg[strid]] = set()
                        reasons[msg[strid]].add('Long operation')
                        break
            # Check if message is related to the VM commands
            for field in msg:
                for thread in (vm_tasks.keys()):
                    for task in vm_tasks[thread]:
                        if task['command_name'] in str(field):
                            needed_msgs.add(msg[strid])
                            if msg[strid] not in reasons.keys():
                                reasons[msg[strid]] = set()
                            reasons[msg[strid]].add('VM command')
        #exit()
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
                    reasons[msg[strid]] = set()
                reasons[msg[strid]].add('Differ by VM IDs')
        if len(set([len(w) for w in diff])) == 1:
            # Show because cluster differs by one-length words (usually IDs)
            for msg in clusters[c_id]:
                needed_msgs.add(msg[strid])
                if msg[strid] not in reasons.keys():
                    reasons[msg[strid]] = set()
                reasons[msg[strid]].add('One-length IDs')
        else:
            # Show because includes keyword
            for msg in clusters[c_id]:
                for field in msg:
                    if any([k in str(field).lower() for k in
                            ['error ', 'fail', 'traceback',
                             'except', 'warn']]):
                        needed_msgs.add(msg[strid])
                        if msg[strid] not in reasons.keys():
                            reasons[msg[strid]] = set()
                        reasons[msg[strid]].add('Error or warning')
    # endloop
    for t in range(10, len(err_timeline)-10):
        if len(err_timeline[t-5:t])*2 < len(err_timeline[t:t+5]):
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
    for msg in all_errors:
        if msg[strid] in needed_msgs:
            msg_showed += [[msg[dtid], msg[strid],
                            '_'.join(sorted(reasons[msg[strid]])), msg[msid]]]
        else:
            if msg[strid] in reasons.keys():
                reason = '_'.join(sorted(reasons[msg[strid]]))
            else:
                reason = 'unknown'
            f.write("%12s %s | %10s | %s\n" %
                  (datetime.utcfromtimestamp(msg[dtid]).strftime(
                   "%H:%M:%S,%f")[:-3],
                   datetime.utcfromtimestamp(msg[dtid]).strftime(
                   "%d-%m-%Y"),
                   msg[strid], msg[msid]))
    f.close()
    msg_showed = sorted(msg_showed, key=lambda k: k[0])
    prev_message = msg_showed[0][msid]
    for msg in (msg_showed[1:]).copy():
        if msg[msid] == prev_message:
            msg_showed.remove(msg)
        prev_message = msg[msid]
    return msg_showed, new_fields
