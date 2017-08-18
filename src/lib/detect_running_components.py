import os
import re
import lzma
import json
import pytz
import numpy as np
from datetime import datetime
import progressbar


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
    f.seek(0, os.SEEK_END)
    file_len = f.tell()
    multiline = False
    f.seek(pos, os.SEEK_SET)
    dt = 0
    while dt < time_range_info[0]:
        dt = 0
        while dt == 0:
            real_firstpos = f.tell()
            dt = parse_date_time(f.readline(), tz_info)
    f.seek(real_firstpos, os.SEEK_SET)
    bar = progressbar.ProgressBar(redirect_stdout=True, max_value=file_len)
    i = real_firstpos
    for line_num, line in enumerate(f):
        i += len(line)
        bar.update(i)
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
    f.seek(0, os.SEEK_END)
    file_len = f.tell()
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
    bar = progressbar.ProgressBar(redirect_stdout=True, max_value=file_len)
    i = real_firstpos
    for line_num, line in enumerate(f):
        i += len(line)
        bar.update(i)
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
    f.seek(0, os.SEEK_END)
    file_len = f.tell()
    f.seek(pos, os.SEEK_SET)
    dt = 0
    while dt < time_range_info[0]:
        dt = 0
        while dt == 0:
            real_firstpos = f.tell()
            dt = parse_date_time(f.readline(), tz_info)
    f.seek(real_firstpos, os.SEEK_SET)
    bar = progressbar.ProgressBar(redirect_stdout=True, max_value=file_len)
    i = real_firstpos
    for line_num, line in enumerate(f):
        i += len(line)
        bar.update(i)
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
                output_descriptor.write('\n')
            elif 'libvirt' in log.lower():
                vms, hosts, firstline_pos = \
                    libvirtd_vm_host(f, log, 0,  # log_position,
                                     tz_info[log_idx], vms, hosts,
                                     time_range_info[tr_idx])
                output_descriptor.write('\n')
            else:
                vms, hosts, firstline_pos = \
                    engine_vm_host(f, 0,  # log_position,
                                   tz_info[log_idx],
                                   vms, hosts,
                                   time_range_info[tr_idx])
                output_descriptor.write('\n')
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
        # print(k,':id:',[j for j in vms[k]['id']],'\n',k,':found on hosts:',
        #       [j for j in vms[k]['hostids']])
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


def find_vm_tasks_engine(positions, output_descriptor, log_directory,
                         log, file_formats, tz_info, time_range_info,
                         user_vms, user_hosts, additive, output_directory):
    commands_threads = {}
    long_actions = []
    vm_in_threads = set()
    tasks = {}
    commands = {}
    fullname = os.path.join(log_directory, log)
    if (log[-4:] == '.log'):
            f = open(fullname)
    elif (log[-3:] == '.xz'):
        f = lzma.open(fullname, 'rt')
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
            line_name = log + ':' + str(line_num + 1)
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
                vm_in_threads.add(fields["thread"])
            com = re.search(r"\((.+?)\)\ +\[(.*?)\]\ +" +
                            r"[Rr]unning [Cc]ommand:\ +" +
                            r"([^\s]+)[Cc]ommand", line)
            if (com is not None):
                if (com.group(1) not in commands_threads.keys()):
                    commands_threads[com.group(1)] = []
                commands_threads[com.group(1)] += [
                                 {'command_name': com.group(3),
                                  'command_start_name': com.group(3),
                                  'init_time': dt,
                                  'first_line_num': line_name,
                                  'flow_id': com.group(2)}]
                continue
            start = re.search(r"\((.+?)\)\ +\[(.*?)\]\ +" +
                              r"[Ss][Tt][Aa][Rr][Tt],\ +" +
                              r"([^\s]+)Command.*\ +log id:\ (.+)", line)
            if (start is not None):
                if (start.group(1) not in commands_threads.keys()):
                    commands_threads[start.group(1)] = [
                                     {'command_name': start.group(3),
                                      'command_start_name': start.group(3),
                                      'start_time': dt,
                                      'log_id': start.group(4),
                                      'flow_id': start.group(2),
                                      'first_line_num': line_name}]
                else:
                    flow_list = [com['flow_id'] for com in
                                commands_threads[start.group(1)]]
                    try:
                        com_id = len(flow_list) - 1 - \
                                 flow_list[::-1].index(start.group(2))
                        com_name = commands_threads[start.group(1)][com_id][
                                    'command_name']
                        commands_threads[start.group(1)][
                            com_id]['command_start_name'] = start.group(3)
                        commands_threads[start.group(1)][
                            com_id]['start_time'] = dt
                        commands_threads[start.group(1)][
                            com_id]['log_id'] = dt
                    except:
                        commands_threads[start.group(1)] += [
                                        {'command_name': start.group(3),
                                         'command_start_name': start.group(3),
                                         'start_time': dt,
                                         'log_id': start.group(4),
                                         'flow_id': start.group(2),
                                         'first_line_num': line_name}]
                continue
            finish = re.search(r"\((.+?)\)\ +\[(.*?)\]\ +" +
                               r"[Ff][Ii][Nn][Ii][Ss][Hh],\ +" +
                               r"([^\s]+)Command.*\ +log id:\ (.+)", line)
            if (finish is not None):
                if (finish.group(1) not in commands_threads.keys()):
                    continue
                for task_idx, command in \
                        enumerate(commands_threads[finish.group(1)]):
                    if ('log_id' in command.keys() and
                            command['log_id'] == finish.group(4)):
                        commands_threads[finish.group(1)][
                                                task_idx]['finish_time'] = dt
                        commands_threads[finish.group(1)][
                                                task_idx][
                                                'finish_line_num'] = line_name
                        if ('start_time' in commands_threads[
                                finish.group(1)][task_idx].keys()):
                            commands_threads[finish.group(1)][
                                             task_idx]['duration'] = dt - \
                                commands_threads[finish.group(1)][
                                                 task_idx]['start_time']
                            break
                continue
            # ending = re.search(r"\((.+?)\)\ +\[(.*?)\]\ +" +
            #                    r"[Ee]nding\ +[Cc]ommand\ *" +
            #                    r"\'.+\.(.+)Command\'\ *successfully", line)
            # if (ending is not None):
            #     output_descriptor.write('Not None \n')
            #     if (ending.group(1) not in commands_threads.keys()):
            #         output_descriptor.write('Not in commands_threads.keys()\n')
            #         continue
            #     else:
            #         com_list = [com['command_name'] for com in
            #                     commands_threads[ending.group(1)]
            #                     if 'init_time' in com.keys()]
            #         try:
            #             com_id = len(com_list) - 1 - \
            #                      com_list[::-1].index(ending.group(3))
            #         except:
            #             output_descriptor.write('Not found com_id\n')
            #             # commands_threads[ending.group(1)] += [{
            #             #                  'command_name': ending.group(3),
            #             #                  'command_start_name': ending.group(3),
            #             #                  'log_id': ending.group(4),
            #             #                  'flow_id': ending.group(2)}]
            #             continue
            #     #for task_idx, command in \
            #     #        enumerate(commands_threads[ending.group(1)]):
            #     #    if ('log_id' in command.keys() and
            #     #            command['log_id'] == ending.group(4)):
            #         commands_threads[ending.group(1)][
            #                                 com_id]['end_time'] = dt
            #         if ('init_time' in commands_threads[
            #                 ending.group(1)][com_id].keys()):
            #             commands_threads[ending.group(1)][
            #                              com_id]['duration'] = dt - \
            #                 commands_threads[ending.group(1)][
            #                                  com_id]['init_time']
            #         else:
            #             output_descriptor.write('Not found init_time\n')
            #     continue
            # difficulties
            multiasync = re.search(r"\((.+?)\)\ +\[(.*?)\].+" +
                                   r"[Aa]dding\ +CommandMultiAsyncTasks\ +" +
                                   r"[Oo]bject\ +[Ff]or\ +[Cc]ommand\ +" +
                                   r"\'(.+?)\'", line)
            if multiasync is not None:
                commands[multiasync.group(3)] = {'name': commands_threads[
                              multiasync.group(1)][-1]['command_name'],
                              'thread': multiasync.group(1),
                              'flow_id': multiasync.group(2),
                              'first_line_num': line_name}
                continue
            subtask_init = re.search(r"\((.+?)\)\ +\[(.*?)\].+" +
                                r"[Aa]ttaching [Tt]ask\ +\'(.+?)\'\ +" +
                                r"[Tt]o [Cc]ommand\ +\'(.+?)\'",
                                line)
            if subtask_init is not None:
                if (subtask_init.group(4) not in commands.keys()):
                    commands[subtask_init.group(4)] = {
                                            'thread': subtask_init.group(1),
                                            'flow_id': subtask_init.group(2),
                                            'first_line_num': line_name}
                tasks[subtask_init.group(3)] = {
                                            'thread': subtask_init.group(1),
                                            'parent_id': subtask_init.group(4),
                                            'flow_id': subtask_init.group(2),
                                            'first_line_num': line_name}
                continue
            # start
            subtask_start = re.search(r"\((.+?)\)\ +\[(.*?)\].+" +
                                      r"[Aa]dding [Tt]ask\ +\'(.+?)\'\ +" +
                                      r"\(*[Pp]arent [Cc]ommand\ +\'(.+?)\'" +
                                      r".*\)",
                                      line)
            if subtask_start is not None:
                if (subtask_start.group(3) not in tasks.keys()):
                    tasks[subtask_start.group(3)] = {
                                        'parent_name': subtask_start.group(4),
                                        'thread': subtask_start.group(1),
                                        'start_time': dt,
                                        'flow_id': subtask_start.group(2),
                                        'first_line_num': line_name}
                    continue
                tasks[subtask_start.group(3)]['parent_name'] = \
                                                        subtask_start.group(4)
                tasks[subtask_start.group(3)]['start_time'] = dt
                continue
            # wait
            subtask_wait = re.search(r"\((.+?)\)\ +\[(.*?)\].+" +
                                     r"[Cc]ommand\ +\'(.+?)\'\ +\([IDid]+\:" +
                                     r"\ +" +
                                     r"\'(.+?)\'\)\ +[Ww]aiting [Oo]n\ +" +
                                     r"[Cc]hild.+[IDid]\:\ +" +
                                     r"\'(.+?)\'\ +[Tt]ype\:\ *\'(.+?)\'",
                                     line)
            if subtask_wait is not None:
                if (subtask_wait.group(4) not in commands.keys()):
                    commands[subtask_wait.group(4)] = {
                                            'thread': subtask_wait.group(1),
                                            'flow_id': subtask_wait.group(2),
                                            'first_line_num': line_name}
                commands[subtask_wait.group(4)]['name'] = subtask_wait.group(3)
                if (subtask_wait.group(5) in commands.keys()
                        and commands[subtask_wait.group(5)]['name'] != 
                        subtask_wait.group(6)):
                    commands[subtask_wait.group(5)] = {
                                            'name': subtask_wait.group(6),
                                            'thread': 'n/a',
                                            'flow_id': 'n/a',
                                            'first_line_num': 'n/a'}
                if 'childs' not in commands[subtask_wait.group(4)].keys():
                    commands[subtask_wait.group(4)]['childs'] = [
                                    {'child_id': subtask_wait.group(5),
                                     'child_name': subtask_wait.group(6)}]
                    continue
                if (subtask_wait.group(5) not in 
                        [child['child_id'] for child in 
                        commands[subtask_wait.group(4)]['childs']]):
                    commands[subtask_wait.group(4)]['childs'] += [
                                        {'child_id': subtask_wait.group(5),
                                         'child_name': subtask_wait.group(6)}]
                continue
            # end
            subtask_end = re.search(r"\((.+?)\)\ +\[(.*?)\].+" +
                                    r"[Rr]emoved [Tt]ask\ +\'(.+?)\'\ +" +
                                    r"[Ff]rom [Dd]ata[Bb]ase", line)
            if subtask_end is not None:
                if (subtask_end.group(3) not in tasks.keys()):
                    continue
                tasks[subtask_end.group(3)]['end_time'] = dt
                tasks[subtask_end.group(3)]['finish_line_num'] = line_name
                if ('start_time' in tasks[subtask_end.group(3)].keys()):
                    tasks[subtask_end.group(3)]['duration'] = dt - \
                                    tasks[subtask_end.group(3)]['start_time']    
                continue
    f.close()
    command_lvl = {}
    for com in sorted(commands.keys()):
        for task_id in sorted(tasks.keys()):
            if 'parent_id' not in tasks[task_id].keys():
                tasks.pop(task_id)
                continue
            if (tasks[task_id]['parent_id'] == com):
                if 'tasks' not in commands[com].keys():
                    commands[com]['ztasks'] = []
                commands[com]['ztasks'] += [tasks[task_id]]
                commands[com]['ztasks'][-1]['id'] = task_id
                command_lvl[task_id] = 0
                tasks.pop(task_id)
    json.dump(commands_threads, open(os.path.join(output_directory,
                                     log_directory.split('/')[-2] +
                                     '_engine_commands_by_threads.json'),
                                     'w'), indent=4, sort_keys=True)
    new_commands, command_lvl = link_commands(log_directory.split('/')[-2],
                                              output_descriptor,
                                              commands, command_lvl,
                                              output_directory)
    if commands_threads != {}:
        commands_threads, long_actions = select_needed_commands(
                                            commands_threads, vm_in_threads)
    return commands_threads, long_actions, new_commands, command_lvl


def link_commands(log_dir, output_descriptor, commands, command_lvl,
                  output_directory):
    without_parents = []
    new_commands = {}
    for command_id in sorted(commands.keys()):
        if 'childs' not in commands[command_id].keys():
            new_commands[command_id] = commands[command_id]
            new_commands[command_id]['lvl'] = 1
            command_lvl[command_id] = 1
        else:
            for child in commands[command_id]['childs']:
                if child['child_id'] not in commands.keys():
                    commands[command_id]['childs'].remove(child)
            if len(commands[command_id]['childs']) == 0:
                commands[command_id].pop('childs', None)
                new_commands[command_id] = commands[command_id]
                new_commands[command_id]['lvl'] = 1
                command_lvl[command_id] = 1
    leafs = True
    while leafs:
        leafs = False
        for idx, command_id in enumerate(sorted(commands.keys())):
            if 'childs' not in commands[command_id].keys():
                leafs = True
                parent = find_parent(output_descriptor, commands, command_id)
                if parent is None:
                    without_parents += [commands[command_id]]
                    without_parents[-1]['id'] = command_id
                    commands.pop(command_id)
                    new_commands.pop(command_id)
                else:
                    commands.pop(command_id)
                    new_commands[parent] = {
                                        'name': commands[parent]['name'],
                                        'thread': commands[parent]['thread'],
                                        'flow_id': commands[parent]['flow_id'],
                                        'first_line_num': commands[parent][
                                                          'first_line_num'],
                                        'id': parent,
                                        'lvl': 2,
                                        'zchildren': []}
                    command_lvl[parent] = 2
                    for child in commands[parent]['childs']:
                        if (child['child_id'] in new_commands.keys()
                                or child['child_id'] in commands.keys()
                                and 'childs' not in
                                commands[child['child_id']].keys()):
                            new_commands[parent]['zchildren'] += \
                                            [new_commands[child['child_id']]]
                            new_commands[parent]['zchildren'][-1]['id'] = \
                                            child['child_id']
                            if child['child_id'] in commands.keys():
                                commands.pop(child['child_id'])
                            if child['child_id'] in new_commands.keys():
                               new_commands.pop(child['child_id'])
                break
    heads = []
    while (len(commands)>0):
        for idx, command_id in enumerate(sorted(new_commands.keys())):
            if command_id in heads:
                continue
            parent = find_parent(output_descriptor, commands, command_id)
            if parent is None:
                if command_id in commands.keys():
                    commands.pop(command_id)
                heads += [command_id]
                break
            else:
                new_commands[parent] = {'name': commands[parent]['name'],
                                        'thread': commands[parent]['thread'],
                                        'flow_id': commands[parent]['flow_id'],
                                        'first_line_num': commands[parent][
                                                            'first_line_num'],
                                        'id': parent,
                                        'zchildren': []}
                for child in commands[parent]['childs']:
                    if (child['child_id'] in new_commands.keys()
                            or child['child_id'] in commands.keys()
                            and 'childs' not in
                            commands[child['child_id']].keys()):
                        new_commands[parent]['zchildren'] += \
                                            [new_commands[child['child_id']]]
                        new_commands[parent]['zchildren'][-1]['id'] = \
                                            child['child_id']
                        new_commands[parent]['lvl'] = \
                                            new_commands[child['child_id']][
                                            'lvl'] + 1
                        command_lvl[parent] = new_commands[child['child_id']][
                                            'lvl'] + 1
                        if child['child_id'] in commands.keys():
                            commands.pop(child['child_id'])
                        if child['child_id'] in new_commands.keys():
                            new_commands.pop(child['child_id'])
            break
    for com in without_parents:
        new_commands[com['id']] = com
    json.dump(new_commands, open(os.path.join(output_directory,
              log_dir + '_commands.json'), 'w'), indent=4,
              sort_keys=True)
    return new_commands, command_lvl


def find_parent(output_descriptor, commands, command_id):
    parent = None
    for com in sorted(commands.keys()):
        if ('childs' in commands[com].keys()
                and command_id in [c['child_id']
                for c in commands[com]['childs']]):
            parent = com
            break
    return parent

def find_vm_tasks_libvirtd(positions, output_descriptor, log_directory,
                           log, file_formats, tz_info, time_range_info,
                           user_vms, user_hosts, additive, output_directory):
    commands_threads = {}
    long_actions = []
    vm_in_threads = set()
    qemu_monitor = {}
    fullname = os.path.join(log_directory, log)
    if (log[-4:] == '.log'):
            f = open(fullname)
    elif (log[-3:] == '.xz'):
        f = lzma.open(fullname, 'rt')
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
                vm_in_threads.add(fields["thread"])
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
                continue
            #qemu monitor
            send_monitor = re.search(r"mon\ *=\ *(.+?)\ +buf\ *\=\ *" +
                                     r"\{\"execute.+\"id\"\:\ *\"(.+?)\"\}",
                                     line)
            if send_monitor is not None:
                if (send_monitor.group(1) not in qemu_monitor.keys()):
                    qemu_monitor[send_monitor.group(1)] = []
                qemu_monitor[send_monitor.group(1)] += [
                                            {'send_time': dt, 
                                             'id': send_monitor.group(2)}]

            return_monitor = re.search(r"mon\ *=\ *(.+?)\ +buf\ *\=\ *" +
                                       r"\{\"return.+\"id\"\:\ *\"(.+?)\"\}",
                                       line)
            if return_monitor is not None:
                if (return_monitor.group(1) not in qemu_monitor.keys()):
                    continue
                for mes_idx, mes in enumerate(qemu_monitor[
                        return_monitor.group(1)]):
                    if (mes['id'] == return_monitor.group(2)):
                        duration = dt - qemu_monitor[return_monitor.group(1)][
                                                     mes_idx]['send_time']
                        if (duration < 1):
                            qemu_monitor[return_monitor.group(1)].remove(mes)
                            break
                        qemu_monitor[return_monitor.group(1)][
                            mes_idx]['return_time'] = dt
                        qemu_monitor[return_monitor.group(1)][
                            mes_idx]['duration'] = duration
                        break

    f.close()
    json.dump(qemu_monitor, open(os.path.join(output_directory,
                                 log_directory.split('/')[-2] +
                                 '_qemu_libvirt.json'),
                                 'w'), indent=4, sort_keys=True)
    # json.dump(commands_threads, open(os.path.join(output_directory,
    #                                  'tasks_libvirtd_' +
    #                                  log_directory.split('/')[-2] +
    #                                  '.json'),
    #                                  'w'), indent=4, sort_keys=True)
    if commands_threads != {}:
        commands_threads, long_actions = select_needed_commands(
                                            commands_threads, vm_in_threads)
    # json.dump(commands_threads, open(os.path.join(output_directory,
    #                                  'filtered_tasks_libvirtd_' +
    #                                  log_directory.split('/')[-2] +
    #                                  '.json'),
    #                                  'w'), indent=4, sort_keys=True)
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
