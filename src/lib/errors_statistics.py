""" Linking errors from logfiles to each other
"""
import os
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


def clusterize_messages(out_descr, all_errors, fields, user_events,
                        all_vms, all_hosts, subtasks, dirname,
                        err_timeline, vm_tasks,
                        long_tasks, output_directory, detail_reasons,
                        needed_msgs, criterias, vm_timeline):
    reasons = {}
    template = re.compile(r"[^^](\"[^\"]{20,}\")|" +
                          r"[^^](\'[^\']{20,}\')|" +
                          r"[^^](\(+.*\)+)|" +
                          r"[^^](\[+.*\]+)|" +
                          r"[^^](\{+.*\}+)|" +
                          r"[^^](\<+.*\>+)")
    msid = fields.index("message")
    dtid = fields.index('date_time')
    strid = fields.index('line_num')
    fields += ['filtered']
    events = {}
    all_errors = [msg for msg in all_errors if len(msg[msid]) > 10]
    vms_list = list(all_vms.keys()) + [i for k in all_vms.keys()
                                       for i in all_vms[k]['id']]
    hosts_list = list(all_hosts.keys()) + [i for k in all_hosts.keys()
                                           for i in all_hosts[k]['id']]
    for err_id in range(len(all_errors)):
        if err_id % 100 == 0:
            out_descr.write(('clusterize_messages: Preprocessing %s ' +
                             'from %s\r') % (err_id, len(all_errors)))
        mstext = all_errors[err_id][msid]
        groups = re.findall(template, mstext)
        for g in groups:
            for subg in g:
                if subg == '' or any([k in subg
                                      for k in
                                      vms_list + hosts_list]):
                    continue
                mstext = mstext.replace(subg, '')
        mstext = mstext[:min(80, len(mstext))]
        if mstext not in events.keys():
            events[mstext] = {'date_time': [], 'line_num': [], 'data': [],
                              'keywords': set()}
        events[mstext]['date_time'] += [all_errors[err_id][dtid]]
        events[mstext]['line_num'] += [all_errors[err_id][strid]]
        events[mstext]['data'] += [all_errors[err_id]]
        all_errors[err_id] += [mstext]
        for event in user_events:
            if event in all_errors[err_id][msid]:
                needed_msgs.add(all_errors[err_id][strid])
                if all_errors[err_id][strid] not in detail_reasons.keys():
                    detail_reasons[all_errors[err_id][strid]] = set()
                detail_reasons[all_errors[err_id][strid]].add('Event=' +
                                                              event)
        for vm_name in vms_list:
            if (vm_name in all_errors[err_id][msid]):
                if vm_name not in all_vms.keys():
                    vm_add = [k for k in all_vms.keys()
                              for vm in all_vms[k]['id']
                              if vm == vm_name]
                else:
                    vm_add = [vm_name]
                needed_msgs.add(all_errors[err_id][strid])
                if all_errors[err_id][strid] not in detail_reasons.keys():
                    detail_reasons[all_errors[err_id][strid]] = set()
                detail_reasons[all_errors[err_id][strid]].add('VM=' +
                                                              vm_add[0])
                if 'Differ by VM ID' in criterias:
                    events[mstext]['keywords'].add(vm_name)
        for host_name in hosts_list:
            if (host_name in all_errors[err_id][msid]):
                if host_name not in all_hosts.keys():
                    host_add = [k for k in all_hosts.keys()
                                for host in all_hosts[k]['id']
                                if host == host_name]
                else:
                    host_add = [host_name]
                needed_msgs.add(all_errors[err_id][strid])
                if all_errors[err_id][strid] not in detail_reasons.keys():
                    detail_reasons[all_errors[err_id][strid]] = set()
                detail_reasons[all_errors[err_id][strid]].add('Host=' +
                                                              host_add[0])
        if 'Subtasks' in criterias:
            for t in subtasks.keys():
                if t in all_errors[err_id][msid]:
                    # Check if user-defined words are in the message
                    needed_msgs.add(all_errors[err_id][strid])
                    if all_errors[err_id][strid] not in reasons.keys():
                        reasons[all_errors[err_id][strid]] = set()
                    reasons[all_errors[err_id][strid]].add('Task/' +
                                                           str(subtasks[t]))
                    # break
        if 'Error or warning' in criterias:
            for k in ['error', 'fail', 'failure', 'failed', 'traceback',
                      'warn', 'warning', 'could not', 'exception', 'down',
                      'crash']:
                for field_id, f in enumerate(fields):
                    err_res = re.search(r'(^|[ \:\.\,]+)' + k +
                                        r'([ \:\.\,=]+|$)',
                                        str(all_errors[err_id][
                                            field_id]).lower())
                    if err_res is not None:
                        # if (k == 'traceback' or f != 'message'):
                        needed_msgs.add(all_errors[err_id][strid])
                        if all_errors[err_id][strid] not in reasons.keys():
                            reasons[all_errors[err_id][strid]] = set()
                        reasons[all_errors[err_id][strid]].add(
                            'Error or warning')
    new_events = {}
    for shorten in sorted(events.keys()):
        word_key = shorten.split(' ')[0]
        if len(shorten.split(' ')) > 1:
            word_key += ' '+shorten.split(' ')[1]
        if word_key not in new_events.keys():
            new_events[word_key] = {'date_time': [], 'line_num': [],
                                    'data': [], 'keywords': set()}
        new_events[word_key]['date_time'] += events[shorten]['date_time']
        new_events[word_key]['line_num'] += events[shorten]['line_num']
        new_events[word_key]['data'] += events[shorten]['data']
        new_events[word_key]['keywords'].union(events[shorten]['keywords'])
    events = new_events
    del new_events
    f = open(os.path.join(output_directory,
             dirname.split('/')[-2]+"_clusters.txt"), 'w')
    for c_id, clust in enumerate(sorted(events.keys())):
        for mes in events[clust]['data']:
            f.write("%d : %s\n" % (c_id, mes[msid]))
        f.write('\n')
    f.close()
    out_descr.write('\n')
    mean_len = np.mean([len(events[g]['line_num']) for g in events.keys()])
    std_len = np.std([len(events[g]['line_num']) for g in events.keys()])
    for fid, filtered in enumerate(events.keys()):
        out_descr.write(("clusterize_messages: Cluster %d from %d\r") %
                        (fid+1, len(events.keys())))
        if 'Differ by VM ID' in criterias:
            if (len(events[filtered]['keywords']) > 1):
                # Differ by VM
                for line_num in events[filtered]['line_num']:
                    needed_msgs.add(line_num)
                    if line_num not in reasons.keys():
                        reasons[line_num] = set()
                    reasons[line_num].add('Differ by VM IDs')
        if 'Exclude frequent messages' in criterias:
            if len(events[filtered]['line_num']) > mean_len + 3*std_len:
                for line_num in events[filtered]['line_num']:
                    if line_num not in reasons.keys():
                        reasons[line_num] = set()
                    reasons[line_num].add('Many messages')
                    if (line_num in needed_msgs):
                        needed_msgs.remove(line_num)
            elif (len(events[filtered]['line_num']) == 1):
                needed_msgs.add(events[filtered]['data'][0][strid])
                if events[filtered]['data'][0][strid] not in reasons.keys():
                    reasons[events[filtered]['data'][0][strid]] = set()
                reasons[events[filtered]['data'][0][strid]].add('Unique')
            elif (len(events[filtered]['line_num']) < mean_len - 3*std_len):
                for line_num in events[filtered]['line_num']:
                    if line_num not in reasons.keys():
                        reasons[line_num] = set()
                    reasons[line_num].add('Rare')
    out_descr.write('\n')
    if 'Increased errors' in criterias:
        for t in range(10, len(err_timeline)-10):
            if len(err_timeline[t-10:t]) < len(err_timeline[t:t+10]):
                # Show because an amount of followed messages increased
                for msg in err_timeline[t]:
                    needed_msgs.add(msg[strid])
                    if msg[strid] not in reasons.keys():
                        reasons[msg[strid]] = set()
                    reasons[msg[strid]].add('Increased errors')
    msg_showed = []
    new_fields = ['date_time', 'line_num', 'reason', 'details', 'message']
    if reasons == {}:
        return msg_showed, new_fields
    f = open(os.path.join(output_directory,
             dirname.split('/')[-2]+'_frequent.txt'), 'w')
    separator = ';'
    max_len = max([len(separator.join(reasons[r])) for r in reasons.keys()])
    for msg in all_errors:
        if msg[strid] in needed_msgs:
            if msg[strid] in reasons.keys():
                all_reasons = separator.join(sorted(reasons[msg[strid]]))
            else:
                all_reasons = ''
            if msg[strid] in detail_reasons.keys():
                all_details = separator.join(
                    sorted(detail_reasons[msg[strid]])
                )
            else:
                all_details = ''
            msg_showed += [[msg[dtid], msg[strid],
                            all_reasons,
                            all_details,
                            msg[msid]]]
        else:
            if msg[strid] in reasons.keys():
                reason = separator.join(sorted(reasons[msg[strid]]))
            else:
                reason = 'unknown'
            f.write("%12s %s | %20s | %*s | %s\n" %
                    (datetime.utcfromtimestamp(msg[dtid]).strftime(
                                                        "%H:%M:%S,%f")[:-3],
                     datetime.utcfromtimestamp(msg[dtid]).strftime(
                                                        "%d-%m-%Y"),
                     msg[strid], max_len, reason, msg[msid]))
    f.close()
    strid = new_fields.index('message')
    msg_showed = sorted(msg_showed, key=lambda k: k[0])
    prev_message = msg_showed[0][strid]
    for msg in (msg_showed[1:]).copy():
        if msg[strid] == prev_message:
            msg_showed.remove(msg)
        prev_message = msg[strid]
    return msg_showed, new_fields
