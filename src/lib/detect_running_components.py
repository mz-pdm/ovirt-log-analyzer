import os
import re
import lzma
import json
import pytz
import numpy as np
from datetime import datetime


re_timestamp = re.compile(
        r"[0-9\-]{10}[\sT][0-9]{2}:[0-9]{2}:[0-9]{2}[\.\,0-9]*[\+\-0-9Z]*")
dt_formats = ["%Y-%m-%d %H:%M:%S,%f%z", "%Y-%m-%d %H:%M:%S%z"]


def parse_date_time(line, time_zone):
    dt = re_timestamp.search(line)
    if dt is None:
        return 0
    dt = dt.group(0)
    dt = dt.replace('T', ' ')
    dt = dt.replace('Z', '+0000')
    dt = dt.replace('.', ',')
    time_part = dt.partition(' ')[2]
    # for "2017-05-12 03:23:31,135-04" format
    if (any([sign in time_part
            and len(time_part.partition(sign)[2]) == 2
            for sign in ['+', '-']])):
        dt += '00'
    elif not any([sign in time_part for sign in ['+', '-']]):
        # if we have time without time zone
        dt += time_zone
    date_time = ''
    for dt_format in dt_formats:
        try:
            date_time = datetime.strptime(dt, dt_format)
            date_time = date_time.astimezone(pytz.utc)
            date_time = date_time.timestamp()
            break
        except ValueError:
            continue
    if date_time == '':
        return 0
    return date_time


def find_time_range(output_descriptor, log_directory, files, tz_info,
                    time_range_info):
    logs_datetimes = {}
    relevant_logs = []
    for log_idx, log in enumerate(files):
        full_filename = os.path.join(log_directory, log)
        if log[-4:] == '.log':
                f = open(full_filename, 'rt')
        elif log[-3:] == '.xz':
            f = lzma.open(full_filename, 'rt')
        else:
            output_descriptor.write("Unknown file extension: %s" % log)
            continue
        logs_datetimes[log] = []
        dt = 0
        while dt == 0:
            dt = parse_date_time(f.readline(), tz_info[log_idx])
        logs_datetimes[log] += [dt]
        f.seek(0, os.SEEK_END)
        file_len = f.tell()
        offset = 1
        dt = 0
        while dt == 0:
            while f.read(1) != "\n":
                offset += 1
                f.seek(file_len-offset, os.SEEK_SET)
            dt = parse_date_time(f.readline(), tz_info[log_idx])
        logs_datetimes[log] += [dt]
        if (logs_datetimes[log][1] < logs_datetimes[log][0]):
            output_descriptor.write(('Warning: %s - end datetime (%s) is ' +
                                     'less than start time (%s)\n') %
                                    (log, datetime.utcfromtimestamp(
                                        logs_datetimes[log][1]).strftime(
                                                "%Y-%m-%dT%H:%M:%S,%f")[:-3],
                                     datetime.utcfromtimestamp(
                                        logs_datetimes[log][0]).strftime(
                                                "%Y-%m-%dT%H:%M:%S,%f")[:-3]))
            output_descriptor.write('\n')
        elif (time_range_info != []
              and (all([logs_datetimes[log][0] > tr[1]
                       for tr in time_range_info])
              or all([logs_datetimes[log][1] < tr[0]
                      for tr in time_range_info]))):
            output_descriptor.write(('Warning: log file "%s" (%s %s) is ' +
                                     'not in the defined time range') %
                                    (log, datetime.utcfromtimestamp(
                                        logs_datetimes[log][1]).strftime(
                                            "%Y-%m-%dT%H:%M:%S,%f")[:-3],
                                     datetime.utcfromtimestamp(
                                        logs_datetimes[log][1]).strftime(
                                            "%Y-%m-%dT%H:%M:%S,%f")[:-3]))
            output_descriptor.write('\n')
        else:
            relevant_logs += [log]
    if relevant_logs == []:
        output_descriptor.write('All log files are outside the defined ' +
                                'time range\n')
        return logs_datetimes, relevant_logs
    if len(relevant_logs) == 1:
        return logs_datetimes, relevant_logs
    for log in logs_datetimes.keys():
        if (sum([logs_datetimes[log][0] - logs_datetimes[log2][1] < 3600
                for log2 in logs_datetimes.keys()]) == 1):
            output_descriptor.write('\nWarning: log files does not cross by ' +
                                    'date time\n\n')
    return logs_datetimes, relevant_logs


def find_needed_linenum(output_descriptor, log_directory, files, tz_info,
                        time_range_info):
    needed_linenum = {}
    for log_idx, log in enumerate(files):
        full_filename = os.path.join(log_directory, log)
        if log[-4:] == '.log':
                f = open(full_filename, 'rt')
        elif log[-3:] == '.xz':
            f = lzma.open(full_filename, 'rt')
        else:
            output_descriptor.write("Unknown file extension: %s" % log)
            continue
        needed_linenum[log] = []
        if time_range_info == []:
            f.seek(0, os.SEEK_END)
            needed_linenum[log] += [0]
            continue
        for tr_idx in range(len(time_range_info)):
            f.seek(0, os.SEEK_SET)
            needed_time = time_range_info[tr_idx][0]
            dt = 0
            while dt == 0:
                cur_pos = f.tell()
                dt = parse_date_time(f.readline(), tz_info[log_idx])
            if (dt >= needed_time and dt < time_range_info[tr_idx][1]):
                needed_linenum[log] += [cur_pos]
                continue
            cur_time = dt
            prev_time = dt
            f.seek(0, os.SEEK_END)
            file_len = f.tell()
            dt = 0
            offset = 1
            while dt == 0:
                while f.read(1) != "\n":
                    offset += 1
                    f.seek(file_len-offset, os.SEEK_SET)
                cur_pos = f.tell()
                dt = parse_date_time(f.readline(), tz_info[log_idx])
            prev_pos = 0
            cur_pos = 0
            next_pos = file_len//2
            condition = False
            while not condition:
                f.seek(next_pos, os.SEEK_SET)
                offset = 1
                while f.read(1) != "\n" and next_pos-offset >= 0:
                    offset += 1
                    f.seek(next_pos-offset, os.SEEK_SET)
                prev_pos = cur_pos
                dt = 0
                while dt == 0:
                    cur_pos = f.tell()
                    dt = parse_date_time(f.readline(), tz_info[log_idx])
                prev_time = cur_time
                cur_time = dt
                if cur_time >= needed_time:
                    next_pos = cur_pos//2
                else:
                    next_pos = file_len - (file_len - cur_pos)//2
                condition = (prev_time <= needed_time) \
                    and (cur_time > needed_time) \
                    or (prev_time > needed_time) \
                    and (cur_time <= needed_time) \
                    or (cur_time == prev_time)
            needed_linenum[log] += [min(prev_pos, cur_pos)]
    return needed_linenum


def libvirtd_vm_host(f, filename, pos, tz_info, vms, hosts,
                     time_range_info):
    cur = {}
    multiline = False
    f.seek(pos, os.SEEK_SET)
    dt = 0
    while dt < time_range_info[0]:
        dt = 0
        while dt == 0:
            real_firstpos = f.tell()
            dt = parse_date_time(f.readline(), tz_info)
    f.seek(real_firstpos, os.SEEK_SET)
    for line_num, line in enumerate(f):
        dt = parse_date_time(line, tz_info)
        if dt == 0:
            continue
        # if dt > time_range_info[1]:
        #     break
        vm_name = re.search(r'\<name\>(.+?)\<\/name\>', line)
        if (not multiline and vm_name is not None):
            multiline = True
            cur['vm_name'] = vm_name.group(1)
            continue
        vm_id = re.search(r'\<uuid\>(.+?)\<\/uuid\>', line)
        if (multiline and vm_id is not None):
            cur['vm_id'] = vm_id.group(1)
            continue
        host_name = re.search(r'\<hostname\>(.+?)\<\/hostname\>', line)
        if (multiline and host_name is not None):
            cur['host_name'] = host_name.group(1)
            continue
        host_id = re.search(r'\<hostuuid\>(.+?)\<\/hostuuid\>', line)
        if (multiline and host_id is not None):
            cur['host_id'] = host_id.group(1)
            if (cur['vm_name'] not in vms.keys()):
                vms[cur['vm_name']] = {'id': set(), 'hostids': set()}
            vms[cur['vm_name']]['id'].add(cur['vm_id'])
            vms[cur['vm_name']]['hostids'].add(cur['host_id'])
            if (cur['host_name'] not in hosts.keys()):
                hosts[cur['host_name']] = {'id': set(), 'vmids': set()}
            hosts[cur['host_name']]['id'].add(cur['host_id'])
            hosts[cur['host_name']]['vmids'].add(cur['vm_id'])
            multiline = False
            cur = {}
            continue
        if multiline:
            # host was not found
            if (cur != {} and 'vm_name' in cur.keys() and
                    'vm_id' in cur.keys()):
                if (cur['vm_name'] not in vms.keys()):
                    vms[cur['vm_name']] = {'id': set(), 'hostids': set()}
                vms[cur['vm_name']]['id'].add(cur['vm_name'])
            multiline = False
            cur = {}
            continue
        # Other types
        other_vm = re.search(r'\(VM\: name=(.+?), uuid=(.+?)\)', line)
        if other_vm is None:
            other_vm = re.search(r'vm=(.+?), uuid=(.+?)\,', line)
        if other_vm is not None:
            if (other_vm.group(1) not in vms.keys()):
                vms[other_vm.group(1)] = {'id': set(), 'hostids': set()}
            vms[other_vm.group(1)]['id'].add(other_vm.group(2))
    return vms, hosts, real_firstpos


def vdsm_vm_host(f, filename, pos, tz_info, vms, hosts,
                     time_range_info):
    cur = {}
    this_host = ''
    multiline = False
    f.seek(pos, os.SEEK_SET)
    dt = 0
    while dt < time_range_info[0]:
        dt = 0
        while dt == 0:
            real_firstpos = f.tell()
            dt = parse_date_time(f.readline(), tz_info)
    f.seek(real_firstpos, os.SEEK_SET)
    for line_num, line in enumerate(f):
        dt = parse_date_time(line, tz_info)
        if dt == 0:
            continue
        # if dt > time_range_info[1]:
        #     break
        vdsm_host = re.search(r'I am the actual vdsm ' +
                              r'([^\ ]+)\ +([^\ ]+)', line)
        if vdsm_host is not None:
            this_host = vdsm_host.group(2)
            if (this_host not in hosts.keys()):
                hosts[this_host] = {'id': set(), 'vmids': set()}        
        vm_name = re.search(r'\<name\>(.+?)\<\/name\>', line)
        if (not multiline and vm_name is not None):
            multiline = True
            cur['vm_name'] = vm_name.group(1)
            continue
        vm_id = re.search(r'\<uuid\>(.+?)\<\/uuid\>', line)
        if (multiline and vm_id is not None):
            cur['vm_id'] = vm_id.group(1)
            if (cur['vm_name'] not in vms.keys()):
                vms[cur['vm_name']] = {'id': set(), 'hostids': set()}
            vms[cur['vm_name']]['id'].add(cur['vm_id'])
            if this_host != '':
                vms[cur['vm_name']]['hostids'].add(this_host)
                hosts[this_host]['vmids'].add(cur['vm_id'])
            multiline = False
            cur = {}
            continue
        other_vm = re.search(
            r'vmId=\'(.+?)\'.+\'vmName\':\ *[u]*\'(.+?)\'', line)
        if other_vm is not None:
            if (other_vm.group(2) not in vms.keys()):
                vms[other_vm.group(2)] = {'id': set(), 'hostids': set()}
            vms[other_vm.group(2)]['id'].add(other_vm.group(1))
            if this_host != '':
                vms[other_vm.group(2)]['hostids'].add(this_host)
                hosts[this_host]['vmids'].add(other_vm.group(1))
    return vms, hosts, real_firstpos

def engine_vm_host(f, pos, tz_info, vms, hosts, time_range_info):
    f.seek(pos, os.SEEK_SET)
    dt = 0
    while dt < time_range_info[0]:
        dt = 0
        while dt == 0:
            real_firstpos = f.tell()
            dt = parse_date_time(f.readline(), tz_info)
    f.seek(real_firstpos, os.SEEK_SET)
    for line_num, line in enumerate(f):
        dt = parse_date_time(line, tz_info)
        if dt == 0:
            continue
        vm_name = ''
        vm_id = ''
        host_name = ''
        host_id = ''
        # if dt > time_range_info[1]:
        #     break
        if any([v in line.lower() for v in ['vmid', 'vmname', 'vm_name']]):
            if (re.search(r"vmId=\'(.+?)\'", line) is not None):
                vm_id = re.search(r"vmId=\'(.+?)\'", line).group(1)
            elif (re.search(r"\'vmId\'\ *[:=]\ *u*\'(.+?)\'",
                            line) is not None):
                vm_id = re.search(
                    r"\'vmId\'\ *[:=]\ *u*\'(.+?)\'", line).group(1)
            else:
                vm_id = ''
            if (re.search(r"vmName\ *=\ *(.+?),", line) is not None):
                vm_name = re.sub('[\'\"]', '',
                                  re.search(r"vmName\ *=\ *(.+?),",
                                            line).group(1))
            elif (re.search(r"vm\ *=\ *\'VM\ *\[([^\[\]]*?)\]\'",
                            line) is not None):
                vm_name = re.sub('[\'\"]', '',
                                  re.search(r"vm\ *=\ *\'VM\ *" +
                                            r"\[([^\[\]]*?)\]\'",
                                            line).group(1))
            elif (re.search(r"\[(.+?)=VM_NAME\]", line) is not None):
                vm_name = re.sub('[\'\"]', '',
                                  re.search(r"\[([^\[\]]*?)=VM_NAME\]",
                                            line).group(1))
            elif (re.search(r"\[(.+?)=VM\]", line) is not None):
                vm_name = re.sub('[\'\"]', '',
                                  re.search(r"\[([^\[\]]*?)=VM\]",
                                            line).group(1))
            elif (re.search(r"\'vmName\'\ *[:=]\ *u*\'([^\']*?)\'",
                            line) is not None):
                vm_name = re.sub('[\'\"]', '',
                                  re.search(r"\'vmName\'" +
                                            r"\ *[:=]\ *u*\'([^\']*?)\'",
                                            line).group(1))
            else:
                vm_name = ''
        if (vm_name == '' and vm_id == ''):
            other_vm = re.search(r'VM\ *\'(.{30,40}?)\'\ *' +
                             r'\(([^\(\)]*?)\)', line)
            if (other_vm is not None):
                vm_name = other_vm.group(2)
                vm_id = other_vm.group(1)
        if (any([i in line.lower() for i in ['hostid',
                                             'hostname']])):
            host_name = re.search(r"HostName\ *=\ *(.+?),", line)
            if host_name is not None:
                host_name = host_name.group(1)
            else:
                host_name = ''
            host_id = re.search(r"hostId=\'(.+?)\'", line)
            if host_id is not None:
                host_id = host_id.group(1)
            else:
                host_id = ''
        if vm_name.lower() == 'null':
            vm_name = ''
        if vm_id.lower() == 'null':
            vm_id = ''
        if host_id.lower() == 'null':
            host_id = ''
        if host_name.lower() == 'null':
            host_name = ''
        if vm_name not in vms.keys():
            vms[vm_name] = {'id': set(), 'hostids': set()}
        vms[vm_name]['id'].add(vm_id)
        vms[vm_name]['hostids'].add(host_name)
        if host_name not in hosts.keys():
            hosts[host_name] = {'id': set(), 'vmids': set()}
        hosts[host_name]['id'].add(host_id)
        hosts[host_name]['vmids'].add(vm_id)
    return vms, hosts, real_firstpos


def find_all_vm_host(positions,
                     output_descriptor,
                     log_directory,
                     files,
                     tz_info,
                     time_range_info):
    vms = {}
    hosts = {}
    # list of number of first lines for the time range to pass others
    first_lines = {}
    for log_idx, log in enumerate(files):
        full_filename = os.path.join(log_directory, log)
        if log[-4:] == '.log':
                f = open(full_filename)
        elif log[-3:] == '.xz':
            f = lzma.open(full_filename, 'rt')
        else:
            output_descriptor.write("Unknown file extension: %s" % log)
            continue
        first_lines[log] = []
        for tr_idx, log_position in enumerate(positions[log]):
            if 'vdsm' in log.lower():
                vms, hosts, firstline_pos = \
                    vdsm_vm_host(f, log, 0,  # log_position,
                                     tz_info[log_idx], vms, hosts,
                                     time_range_info[tr_idx])
            elif 'libvirt' in log.lower():
                vms, hosts, firstline_pos = \
                    libvirtd_vm_host(f, log, 0,  # log_position,
                                     tz_info[log_idx], vms, hosts,
                                     time_range_info[tr_idx])
            else:
                vms, hosts, firstline_pos = \
                    engine_vm_host(f, 0,  # log_position,
                                   tz_info[log_idx],
                                   vms, hosts,
                                   time_range_info[tr_idx])
            first_lines[log] += [firstline_pos]
        f.close()
    # print('------VMS------')
    not_running_vms = []
    for k in sorted(vms.keys()):
        if '' in vms[k]['id']:
            vms[k]['id'].remove('')
        if (len(vms[k]['id']) == 0):
            not_running_vms += [k]
            vms.pop(k)
    not_found_vmnames = []
    for k in sorted(vms.keys()):
        if ('' in vms[k]['hostids']):
            vms[k]['hostids'].remove('')
        if k in not_running_vms and len(vms[k]['id']) > 0:
            not_running_vms.remove(k)
        if k == '':
            not_found_vmnames = list(vms[k]['id'])
            if '' in not_found_vmnames:
                not_found_vmnames.remove('')
            vms.pop(k)
            continue
        if not_found_vmnames != []:
            for v in not_found_vmnames.copy():
                if v in list(vms[k]['id']):
                    not_found_vmnames.remove(v)
        # print(k,':id:',[j for j in vms[k]['id']],'\n',k,':found on hosts:',[j for j in vms[k]['hostids']])
        # print('---')
    # print('Not running VMs:', not_running_vms)
    # print('Not found VM names:', not_found_vmnames)
    # print()
    not_found_hostnames = []
    # print('------HOSTS------')
    for k in sorted(hosts.keys()):
        if k == '':
            not_found_hostnames = list(hosts[k]['id'])
            if '' in not_found_hostnames:
                not_found_hostnames.remove('')
            hosts.pop(k)
            continue
        if not_found_hostnames != []:
            for i in not_found_hostnames.copy():
                if i in list(hosts[k]['id']):
                    not_found_hostnames.remove(i)
        if ('' in hosts[k]['id']):
            hosts[k]['id'].remove('')
        if ('' in hosts[k]['vmids']):
            hosts[k]['vmids'].remove('')
        # print(k,':id:',[j for j in hosts[k]['id']],'\n',k,':found vms:',[j for j in hosts[k]['vmids']])
        # print()
    # print('Not found host names:',not_found_hostnames)
    # exit()
    return vms, hosts, not_running_vms, not_found_vmnames, \
            not_found_hostnames, first_lines


def find_vm_tasks_engine(positions,
                         output_descriptor,
                         log_directory,
                         log,
                         file_formats,
                         tz_info,
                         time_range_info,
                         user_vms, user_hosts, additive):
    commands_threads = {}
    long_actions = []
    needed_threads = set()
    subtasks = []
    subtasks_ids = []
    if (log[-4:] == '.log'):
            f = open(log)
    elif (log[-3:] == '.xz'):
        f = lzma.open(log, 'rt')
    else:
        output_descriptor.write("Unknown file extension: %s" % log)
        return commands_threads, long_actions
    firstline = f.readline()
    for fmt in file_formats:
        prog = re.compile(fmt)
        fields = prog.search(firstline)
        if fields is not None:
            file_format = prog
            break
    if fields is None:
        # Format is not found
        return commands_threads, long_actions
    for tr_idx, pos in enumerate(positions):
        f.seek(pos, os.SEEK_SET)
        for line_num, line in enumerate(f):
            fields = file_format.search(line)
            if fields is None:
                # Tracebacks will be added anyway
                continue
            fields = fields.groupdict()
            dt = parse_date_time(line, tz_info)
            if dt == 0:
                continue
            if (dt > time_range_info[tr_idx][1]):
                break
            if additive:
                condition = (any([vm in fields["message"]
                                  for vm in user_vms]) \
                             or any([host in fields["message"]
                                     for host in user_hosts]))
            else:
                condition = (any([vm in fields["message"]
                                  for vm in user_vms]) \
                             and any([host in fields["message"]
                                     for host in user_hosts]))
            if (condition):
                needed_threads.add(fields["thread"])
            com = re.search(r"\((.+?)\)\ +\[(.*?)\]\ +" +
                            r"[Rr]unning [Cc]ommand:\ +" +
                            r"([^\s]+)[Cc]ommand", line)
            if (com is not None):
                if (com.group(1) not in commands_threads.keys()):
                    commands_threads[com.group(1)] = []
                commands_threads[com.group(1)] += [
                                 {'command_name': com.group(3),
                                  'command_start_name': com.group(3),
                                  'subtasks': []}]
                continue
            start = re.search(r"\((.+?)\)\ +\[(.*?)\]\ +" +
                              r"[Ss][Tt][Aa][Rr][Tt],\ +" +
                              r"([^\s]+)Command.*\ +log id:\ (.+)", line)
            if (start is not None):
                if (start.group(1) not in commands_threads.keys()):
                    commands_threads[start.group(1)] = [
                                     {'command_name': start.group(3),
                                      'command_start_name': start.group(3),
                                      'subtasks': [],
                                      'start_time': dt,
                                      'log_id': start.group(4),
                                      'flow_id': start.group(2)}]
                else:
                    com_list = [com['command_name'] for com in
                                commands_threads[start.group(1)]]
                    try:
                        com_id = len(com_list) - 1 - \
                                 com_list[::-1].index(start.group(3))
                        com_name = commands_threads[start.group(1)][com_id][
                                    'command_name']
                        commands_threads[start.group(1)][com_id] = \
                            {'command_name': com_name,
                             'command_start_name': start.group(3),
                             'subtasks': [],
                             'start_time': dt,
                             'log_id': start.group(4),
                             'flow_id': start.group(2)}
                    except:
                        commands_threads[start.group(1)] += [
                                         {'command_name': start.group(3),
                                          'command_start_name': start.group(3),
                                          'subtasks': [],
                                          'start_time': dt,
                                          'log_id': start.group(4),
                                          'flow_id': start.group(2)}]
                continue
            finish = re.search(r"\((.+?)\)\ +\[(.*?)\]\ +" +
                               r"[Ff][Ii][Nn][Ii][Ss][Hh],\ +" +
                               r"([^\s]+)Command.*\ +log id:\ (.+)", line)
            if (finish is not None):
                if (finish.group(1) not in commands_threads.keys()):
                    commands_threads[finish.group(1)] = [
                                     {'command_name': finish.group(3),
                                      'command_start_name': finish.group(3),
                                      'subtasks': [],
                                      'log_id': finish.group(4),
                                      'flow_id': finish.group(2)}]
                    # com_id = 0
                    continue
                else:
                    com_list = [com['command_name'] for com in
                                commands_threads[finish.group(1)]]
                    try:
                        com_id = len(com_list) - 1 - \
                                 com_list[::-1].index(finish.group(3))
                    except:
                        commands_threads[finish.group(1)] += [{
                                         'command_name': finish.group(3),
                                         'command_start_name': finish.group(3),
                                         'subtasks': [],
                                         'log_id': finish.group(4),
                                         'flow_id': finish.group(2)}]
                        continue
                for task_idx, command in \
                        enumerate(commands_threads[finish.group(1)]):
                    if ('log_id' in command.keys() and
                            command['log_id'] == finish.group(4)):
                        commands_threads[finish.group(1)][
                                                task_idx]['finish_time'] = dt
                        if ('start_time' in commands_threads[
                                finish.group(1)][task_idx].keys()):
                            commands_threads[finish.group(1)][
                                             task_idx]['duration'] = \
                                commands_threads[finish.group(1)][
                                                 task_idx]['finish_time'] - \
                                commands_threads[finish.group(1)][
                                                 task_idx]['start_time']
                            break
            subtask_start = re.search(r"\((.+?)\)\ +\[.*?\].+?" +
                                r"[Aa]dding [Tt]ask\ +\'(.+?)\'\ +" +
                                r"\(*[Pp]arent [Cc]ommand\ +\'(.+?)\'.*\)",
                                line)
            if subtask_start is not None:
                if (subtask_start.group(1) not in commands_threads.keys()):
                    continue
                for task_idx, command in \
                        enumerate(commands_threads[subtask_start.group(1)]):
                    if (command['command_name'] in subtask_start.group(3)):
                        commands_threads[subtask_start.group(1)][
                                task_idx]['subtasks'] += [
                                {'id': subtask_start.group(2),
                                 'start_time': dt}]
                        subtasks += [{'id': subtask_start.group(2),
                                      'parent_thread': subtask_start.group(1),
                                      'task_idx': task_idx}]
                        subtasks_ids += [subtask_start.group(2)]
                        break
            subtask_end = re.search(r"[Rr]emoved [Tt]ask\ +\'(.+?)\'\ +" +
                                    r"[Ff]rom [Dd]ata[Bb]ase", line)
            if subtask_end is None:
                continue
            for subtask_idx, subtask in enumerate(subtasks.copy()):
                if subtask['id'] == subtask_end.group(1):
                    for subt_idx, existed_subt in enumerate(commands_threads[
                            subtask['parent_thread']][
                            subtask['task_idx']]['subtasks']):
                        if (existed_subt['id'] == subtask_end.group(1)):
                            commands_threads[subtask['parent_thread']][
                                subtask['task_idx']]['subtasks'][
                                    subt_idx]['end_time'] = dt
                            commands_threads[subtask['parent_thread']][
                                subtask['task_idx']]['subtasks'][
                                    subt_idx]['duration'] = dt - \
                                        commands_threads[subtask[
                                        'parent_thread']][
                                        subtask['task_idx']]['subtasks'][
                                        subt_idx]['start_time']
                    subtasks.remove(subtask)
    f.close()
    json.dump(commands_threads, open('tasks_engine_' +
                                     log_directory.split('/')[-2] +
                                     '.json',
                                     'w'), indent=4, sort_keys=True)
    if commands_threads != {}:
        commands_threads, long_actions = select_needed_commands(
                                            commands_threads, needed_threads)
    json.dump(commands_threads, open('filtered_tasks_engine_' +
                                     log_directory.split('/')[-2] +
                                     '.json',
                                     'w'), indent=4, sort_keys=True)
    return commands_threads, long_actions, subtasks_ids


def find_vm_tasks_libvirtd(positions,
                           output_descriptor,
                           log_directory,
                           log,
                           file_formats,
                           tz_info,
                           time_range_info,
                           user_vms, user_hosts, additive):
    commands_threads = {}
    long_actions = []
    needed_threads = set()
    if (log[-4:] == '.log'):
            f = open(log)
    elif (log[-3:] == '.xz'):
        f = lzma.open(log, 'rt')
    else:
        output_descriptor.write("Unknown file extension: %s" % log)
        return commands_threads, long_actions
    firstline = f.readline()
    for fmt in file_formats:
        prog = re.compile(fmt)
        fields = prog.search(firstline)
        if fields is not None:
            file_format = prog
            break
    if fields is None:
        # Format is not found
        return commands_threads, long_actions
    for tr_idx, pos in enumerate(positions):
        f.seek(pos, os.SEEK_SET)
        for line_num, line in enumerate(f):
            fields = file_format.search(line)
            if fields is None:
                # Tracebacks will be added anyway
                continue
            fields = fields.groupdict()
            dt = parse_date_time(line, tz_info)
            if dt == 0:
                continue
            if (dt > time_range_info[tr_idx][1]):
                break
            if additive:
                condition = (any([vm in fields["message"] \
                                  for vm in user_vms]) \
                             or any([host in fields["message"] \
                                     for host in user_hosts]))
            else:
                condition = (any([vm in fields["message"] \
                                  for vm in user_vms]) \
                             and any([host in fields["message"] \
                                      for host in user_hosts]))
            if (condition):
                needed_threads.add(fields["thread"])
            start = re.search(r"Thread (.+?) \((.+?)\) is now running " +
                              r"job (.+)", line)
            if (start is not None):
                if (start.group(1) not in commands_threads.keys()):
                    commands_threads[start.group(1)] = []
                commands_threads[start.group(1)] += [
                                 {'command_name': start.group(3),
                                  'command_start_name': start.group(3),
                                  'start_time': dt}]
                continue
            finish = re.search(r"Thread (.+?) \((.+?)\) finished job (.+?)" +
                               r"( .*|$)", line)
            if (finish is not None):
                if (finish.group(1) not in commands_threads.keys()):
                    commands_threads[finish.group(1)] = [
                                     {'command_name': finish.group(3),
                                      'command_start_name': finish.group(3)}]
                    continue
                else:
                    com_list = [com['command_name'] for com in
                                commands_threads[finish.group(1)]]
                    try:
                        com_id = len(com_list) - 1 - \
                                 com_list[::-1].index(finish.group(3))
                    except:
                        commands_threads[finish.group(1)] += [{
                                         'command_name': finish.group(3),
                                         'command_start_name': finish.group(3)}]
                        com_id = -1
                commands_threads[finish.group(1)][com_id]['finish_time'] = dt
                if ('start_time' in commands_threads[
                        finish.group(1)][com_id].keys()):
                    commands_threads[finish.group(1)][com_id]['duration'] = \
                        commands_threads[finish.group(1)][
                                         com_id]['finish_time'] -\
                        commands_threads[finish.group(1)][
                                         com_id]['start_time']
    f.close()
    json.dump(commands_threads, open('tasks_libvirtd_' +
                                     log_directory.split('/')[-2] +
                                     '.json',
                                     'w'), indent=4, sort_keys=True)
    if commands_threads != {}:
        commands_threads, long_actions = select_needed_commands(
                                            commands_threads, needed_threads)
    json.dump(commands_threads, open('filtered_tasks_libvirtd_' +
                                     log_directory.split('/')[-2] +
                                     '.json',
                                     'w'), indent=4, sort_keys=True)
    return commands_threads, long_actions


def select_needed_commands(all_threads, needed_threads):
    vm_threads = {}
    operations_time = {}
    long_operations = {}
    for thread in all_threads:
        if thread in needed_threads:
            vm_threads[thread] = all_threads[thread]
        for command in all_threads[thread]:
            if ('duration' in command.keys()):
                if command['command_start_name'] not in operations_time.keys():
                    operations_time[command['command_start_name']] = []
                operations_time[command['command_start_name']] += \
                    [{'duration': command['duration'], 'start_time':
                     command['start_time']}]
    # commands_ex_time = [t['duration'] for c in operations_time
    #                     for t in operations_time[c]]
    # ex_median = np.median(commands_ex_time)
    # ex_std = np.std(commands_ex_time)
    for command in sorted(operations_time.keys()):
        com_time = [c_id['duration'] for c_id in operations_time[command]]
        med_com_time = np.median(com_time)
        std_com_time = np.std(com_time)
        for c_id in operations_time[command]:
            if ((c_id['duration'] > med_com_time + 3*std_com_time
                    and c_id['duration'] > 1) or c_id['duration'] > 5):
                if command not in long_operations.keys():
                    long_operations[command] = []
                long_operations[command] += [c_id['start_time']]
    for thread in all_threads:
        for command in all_threads[thread]:
            found = False
            if 'duration' not in command.keys():
                continue
            if command['command_start_name'] not in long_operations.keys():
                continue
            if command['start_time'] not in long_operations[command[
                                            'command_start_name']]:
                continue
            if thread not in vm_threads.keys():
                vm_threads[thread] = [command]
                continue
            for existed_commands in vm_threads[thread]:
                if 'duration' not in existed_commands.keys():
                    continue
                if (existed_commands['command_start_name'] ==
                        command['command_start_name']
                        and existed_commands['start_time'] ==
                        command['start_time']):
                    found = True
            if found:
                continue
            vm_threads[thread] += [command]
    return vm_threads, long_operations
