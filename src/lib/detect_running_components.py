import os
import re
import lzma
import json
import pytz
import numpy as np
from datetime import datetime
import progressbar
from progressbar import ProgressBar


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
    c = 0
    while dt < time_range_info[0]:
        c += 1
        dt = 0
        while dt == 0:
            real_firstpos = f.tell()
            dt = parse_date_time(f.readline(), tz_info)
    f.seek(real_firstpos, os.SEEK_SET)
    widget_style = [filename + ':', progressbar.Percentage(), ' (',
                    progressbar.SimpleProgress(), ')', ' ',
                    progressbar.Bar(), ' ', progressbar.Timer()]
    bar = ProgressBar(widgets=widget_style, max_value=file_len)
    i = real_firstpos
    bar.update(i)
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
    bar.finish()
    return vms, hosts, real_firstpos


def vdsm_vm_host(f, filename, pos, tz_info, vms, hosts, time_range_info):
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
    widget_style = [filename + ':', progressbar.Percentage(), ' (',
                    progressbar.SimpleProgress(), ')', ' ',
                    progressbar.Bar(), ' ', progressbar.Timer()]
    bar = ProgressBar(widgets=widget_style, max_value=file_len,
                      redirect_stdout=True)
    i = real_firstpos
    bar.update(i)
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
    bar.finish()
    return vms, hosts, real_firstpos


def engine_vm_host(f, filename, pos, tz_info, vms, hosts, time_range_info):
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
    widget_style = [filename + ':', progressbar.Percentage(), ' (',
                    progressbar.SimpleProgress(), ')', ' ',
                    progressbar.Bar(), ' ', progressbar.Timer()]
    bar = ProgressBar(widgets=widget_style, max_value=file_len)
    i = real_firstpos
    bar.update(i)
    unknown_vmnames = []
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
        if vm_name == '' and vm_id != '' and host_name != '':
            unknown_vmnames += [[vm_id, host_name]]
        vms[vm_name]['id'].add(vm_id)
        vms[vm_name]['hostids'].add(host_name)
        if host_name not in hosts.keys():
            hosts[host_name] = {'id': set(), 'vmids': set()}
        hosts[host_name]['id'].add(host_id)
        hosts[host_name]['vmids'].add(vm_id)
    bar.finish()
    return vms, unknown_vmnames, hosts, real_firstpos


def timeline_for_engine_vm(output_directory, log_directory, f, filename,
                           output_descriptor, tz_info, vms, hosts):
    all_vms = {}
    for vm in vms.keys():
        all_vms[vm] = {}
        for host in vms[vm]['hostids']:
            all_vms[vm][host] = []
    for line_num, line in enumerate(f):
        dt = parse_date_time(line, tz_info)
        if dt == 0:
            continue
        vm_start = re.search(r'[Mm]essage\:\ +(VM|Guest)\ +([^\ ]+)\ +' +
                             r'(started|was restarted)\ +on\ +[Hh]ost\ +' +
                             r'([^\ ]+?)([\ +\,]+|$)',
                             line)
        if vm_start is not None:
            this_host = ''
            if vm_start.group(2) not in all_vms.keys():
                all_vms[vm_start.group(2)] = {}
            for hostname in hosts.keys():
                if hostname in vm_start.group(4):
                    this_host = hostname
                    break
            if this_host == '':
                continue
            if (this_host not in
                    all_vms[vm_start.group(2)].keys()):
                all_vms[vm_start.group(2)][this_host] = []
            all_vms[vm_start.group(2)][this_host] += [(dt, 'start')]
        # migration
        migration_start = re.search(r'[Mm]essage\:\ +[Mm]igration\ +started' +
                                    r'\ +\(VM\:\ +([^\ ]+),\ +[Ss]ource\:' +
                                    r'\ +([^\ ]+)\,\ +[Dd]estination\:\ +' +
                                    r'([^\ ]+?)[\ +\,]+',
                                    line)
        if migration_start is not None:
            this_host = ''
            if migration_start.group(1) not in all_vms.keys():
                all_vms[migration_start.group(1)] = {}
            if (migration_start.group(2) not in
                    all_vms[migration_start.group(1)].keys()):
                all_vms[migration_start.group(1)][
                        migration_start.group(2)] = []
            all_vms[migration_start.group(1)][
                    migration_start.group(2)] += [(dt, 'migrating_from')]
            for hostname in hosts.keys():
                if hostname in migration_start.group(3):
                    this_host = hostname
                    break
            if this_host == '':
                continue
            if (this_host not in all_vms[migration_start.group(1)].keys()):
                all_vms[migration_start.group(1)][this_host] = []
            all_vms[migration_start.group(1)][
                    this_host] += [(dt, 'migrating_to')]
        # migration completed
        migration_end = re.search(r'[Mm]essage\:\ +[Mm]igration\ +completed' +
                                  r'\ +\(VM\:\ +([^\ ]+),\ +[Ss]ource\:' +
                                  r'\ +([^\ ]+)\,\ +[Dd]estination\:\ +' +
                                  r'([^\ ]+?)[\ +\,]+',
                                  line)
        if migration_end is not None:
            this_host = ''
            if migration_end.group(1) not in all_vms.keys():
                all_vms[migration_end.group(1)] = {}
            if (migration_end.group(2) not in
                    all_vms[migration_end.group(1)].keys()):
                all_vms[migration_end.group(1)][migration_end.group(2)] = []
            all_vms[migration_end.group(1)][
                    migration_end.group(2)] += [(dt, 'migrated_from')]
            for hostname in hosts.keys():
                if hostname in migration_end.group(3):
                    this_host = hostname
                    break
            if this_host == '':
                continue
            if (this_host not in all_vms[migration_end.group(1)].keys()):
                all_vms[migration_end.group(1)][this_host] = []
            all_vms[migration_end.group(1)][this_host] += [(dt, 'migrated_to')]
        # suspend
        vm_suspend = re.search(r'[Mm]essage\:\ +VM\ +([^\ ]+)\ +' +
                               r'on\ +[Hh]ost\ +([^\ ]+)[\ +\,]+is suspended',
                               line)
        if vm_suspend is not None:
            this_host = ''
            if vm_suspend.group(1) not in all_vms.keys():
                all_vms[vm_suspend.group(1)] = {}
            for hostname in hosts.keys():
                if hostname in vm_suspend.group(2):
                    this_host = hostname
                    break
            if this_host == '':
                continue
            if (this_host not in
                    all_vms[vm_suspend.group(1)].keys()):
                all_vms[vm_suspend.group(1)][this_host] = []
            all_vms[vm_suspend.group(1)][this_host] += [(dt, 'suspend')]
        # down
        vm_down = re.search(r'[Mm]essage\:\ +VM\ +([^\ ]+)\ +is [Dd]own',
                            line)
        if vm_down is not None:
            if vm_down.group(1) not in all_vms.keys():
                all_vms[vm_down.group(1)] = {}
            for host in all_vms[vm_down.group(1)].keys():
                all_vms[vm_down.group(1)][host] += [(dt, 'down')]
    if all_vms != {}:
        all_vms = create_time_ranges_for_vms(all_vms)
    json.dump(all_vms, open(os.path.join(output_directory,
                            filename +
                            '_VMs_timeline.json'),
                            'w'), indent=4, sort_keys=True)
    return all_vms


def create_time_ranges_for_vms(vms):
    vm_time_range = {}
    for vm_name in vms.keys():
        vm_time_range[vm_name] = {}
        for host_name in vms[vm_name].keys():
            host_time = []
            cur_range = {}
            for action_id in vms[vm_name][host_name]:
                if (action_id[1] == 'start'
                        or action_id[1] == 'migrating_to'
                        or action_id[1] == 'migrated_to'):
                    if len(cur_range) > 0:
                        pass
                    else:
                        cur_range['start'] = action_id[0]
                elif (action_id[1] == 'down'
                        or action_id[1] == 'migrated_from'
                        or action_id[1] == 'suspend'):
                    if len(cur_range) == 0:
                        pass
                    elif len(cur_range) == 1:
                        cur_range['end'] = action_id[0]
                    else:
                        pass
                if ('start' not in cur_range.keys()
                        and 'end' in cur_range.keys()):
                    pass
                elif ('start' in cur_range.keys()
                        and 'end' in cur_range.keys()):
                    host_time += [[cur_range['start'], cur_range['end']]]
                    cur_range = {}
            if ('start' in cur_range.keys()
                    and 'end' not in cur_range.keys()):
                host_time += [[cur_range['start'], cur_range['start']*2]]
            if host_time != []:
                vm_time_range[vm_name][host_name] = host_time
    return vm_time_range


def find_all_vm_host(positions,
                     output_descriptor,
                     output_directory,
                     log_directory,
                     files,
                     tz_info,
                     time_range_info):
    vms = {}
    hosts = {}
    # list of number of first lines for the time range to pass others
    first_lines = {}
    unknown_vmnames = []
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
                vms, unknown_vmnames, hosts, firstline_pos = \
                    engine_vm_host(f, log, 0,  # log_position,
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
        for vm_idx in unknown_vmnames:
            if vm_idx[0] in vms[k]['id']:
                vms[k]['hostids'].add(vm_idx[1])
    not_found_hostnames = []
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
    vms_timeline = {}
    for log_idx, log in enumerate(files):
        if 'engine' not in log:
            continue
        full_filename = os.path.join(log_directory, log)
        if log[-4:] == '.log':
                f = open(full_filename)
        elif log[-3:] == '.xz':
            f = lzma.open(full_filename, 'rt')
        else:
            output_descriptor.write("Unknown file extension: %s" % log)
            continue
        for tr_idx, log_position in enumerate(positions[log]):
            cur_timeline = timeline_for_engine_vm(output_directory,
                                                  log_directory, f, log,
                                                  output_descriptor,
                                                  tz_info[log_idx], vms,
                                                  hosts)
            vms_timeline.update(cur_timeline)
    return vms, hosts, not_running_vms, not_found_vmnames, \
        not_found_hostnames, first_lines, vms_timeline


def find_vm_tasks_engine(positions, output_descriptor, log_directory,
                         log, file_formats, tz_info, time_range_info,
                         output_directory, needed_linenum, reasons,
                         criterias):
    commands_threads = {}
    long_actions = []
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
    f.seek(0, os.SEEK_END)
    file_len = f.tell()
    widget_style = [log + ':', progressbar.Percentage(), ' (',
                    progressbar.SimpleProgress(), ')', ' ',
                    progressbar.Bar(), ' ', progressbar.Timer()]
    bar = ProgressBar(widgets=widget_style, max_value=file_len)
    for tr_idx, pos in enumerate(positions):
        f.seek(pos, os.SEEK_SET)
        i = pos
        bar.update(i)
        for line_num, line in enumerate(f):
            i += len(line)
            bar.update(i)
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
                                  'log': log,
                                  'init_line_num': line_num + 1,
                                  'flow_id': com.group(2),
                                  'thread': com.group(1)}]
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
                                      'log': log,
                                      'log_id': start.group(4),
                                      'flow_id': start.group(2),
                                      'thread': start.group(1),
                                      'start_line_num': line_num + 1}]
                else:
                    flow_list = [com['flow_id'] for com in
                                 commands_threads[start.group(1)]]
                    try:
                        com_id = len(flow_list) - 1 - \
                                 flow_list[::-1].index(start.group(1))
                        commands_threads[start.group(1)][
                            com_id]['command_start_name'] = start.group(3)
                        commands_threads[start.group(1)][
                            com_id]['start_time'] = dt
                        commands_threads[start.group(1)][
                            com_id]['log_id'] = start.group(4)
                        commands_threads[start.group(1)][
                            com_id]['start_line_num'] = line_num + 1
                    except:
                        commands_threads[start.group(1)] += [
                                        {'command_name': start.group(3),
                                         'command_start_name': start.group(3),
                                         'start_time': dt,
                                         'log': log,
                                         'log_id': start.group(4),
                                         'flow_id': start.group(2),
                                         'thread': start.group(1),
                                         'start_line_num': line_num + 1}]
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
                        commands_threads[finish.group(1)][task_idx][
                            'finish_time'] = dt
                        commands_threads[finish.group(1)][task_idx][
                            'finish_line_num'] = line_num + 1
                        if ('start_time' in commands_threads[
                                finish.group(1)][task_idx].keys()):
                            commands_threads[finish.group(1)][
                                             task_idx]['duration'] = dt - \
                                commands_threads[finish.group(1)][
                                                 task_idx]['start_time']
                            break
                continue
            ending = re.search(r"\((.+?)\)\ +\[(.*?)\]\ +" +
                               r"[Ee]nding\ +[Cc]ommand\ *" +
                               r"\'.+\.(.+)Command\'\ *successfully", line)
            if (ending is not None):
                if (ending.group(1) not in commands_threads.keys()):
                    continue
                else:
                    thr_list = [(com['thread'], idx, com['init_time'])
                                for thr in
                                commands_threads.keys()
                                for idx, com in
                                enumerate(commands_threads[thr])
                                if 'init_time' in com.keys()
                                and 'duration_full' not in com.keys()]
                    com_list = [com['command_name'] for thr in
                                commands_threads.keys()
                                for com in commands_threads[thr]
                                if 'init_time' in com.keys()
                                and 'duration_full' not in com.keys()]
                    to_sort_idx = sorted(range(len(thr_list)),
                                         key=lambda k: thr_list[k][2])
                    thr_list = [thr_list[i] for i in to_sort_idx]
                    com_list = [com_list[i] for i in to_sort_idx]
                    try:
                        com_id = len(com_list) - 1 - \
                                 com_list.index(ending.group(3))
                    except:
                        output_descriptor.write('Not found com_id\n')
                        continue
                    commands_threads[thr_list[com_id][0]][
                                    thr_list[com_id][1]]['end_time'] = dt
                    commands_threads[thr_list[com_id][0]][
                            thr_list[com_id][1]]['end_line_num'] = line_num + 1
                    commands_threads[thr_list[com_id][0]][
                        thr_list[com_id][1]]['duration_full'] = dt - \
                        commands_threads[thr_list[com_id][0]][thr_list[
                            com_id][1]]['init_time']
                continue
            multiasync = re.search(r"\((.+?)\)\ +\[(.*?)\].+" +
                                   r"[Aa]dding\ +CommandMultiAsyncTasks\ +" +
                                   r"[Oo]bject\ +[Ff]or\ +[Cc]ommand\ +" +
                                   r"\'(.+?)\'", line)
            if multiasync is not None:
                commands[multiasync.group(3)] = {'name': commands_threads[
                              multiasync.group(1)][-1]['command_name'],
                              'thread': multiasync.group(1),
                              'flow_id': multiasync.group(2),
                              'log': log,
                              'first_line_num': line_num + 1}
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
                                            'log': log,
                                            'first_line_num': line_num + 1}
                tasks[subtask_init.group(3)] = {
                                            'thread': subtask_init.group(1),
                                            'parent_id': subtask_init.group(4),
                                            'flow_id': subtask_init.group(2),
                                            'log': log,
                                            'first_line_num': line_num + 1}
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
                                        'log': log,
                                        'first_line_num': line_num + 1}
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
                                            'log': log,
                                            'first_line_num': line_num + 1}
                commands[subtask_wait.group(4)]['name'] = subtask_wait.group(3)
                if (subtask_wait.group(5) in commands.keys()
                        and commands[subtask_wait.group(5)]['name'] !=
                        subtask_wait.group(6)):
                    commands[subtask_wait.group(5)] = {
                                            'name': subtask_wait.group(6),
                                            'thread': 'n/a',
                                            'flow_id': 'n/a',
                                            'log': log,
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
                tasks[subtask_end.group(3)]['end_line_num'] = line_num + 1
                if ('start_time' in tasks[subtask_end.group(3)].keys()):
                    tasks[subtask_end.group(3)]['duration'] = dt - \
                        tasks[subtask_end.group(3)]['start_time']
                continue
    f.close()
    bar.finish()
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
                tasks.pop(task_id)
    json.dump(commands_threads, open(os.path.join(output_directory,
                                     log_directory.split('/')[-2] +
                                     '_engine_commands_by_id.json'),
                                     'w'), indent=4, sort_keys=True)
    new_commands, command_lvl = link_commands(log_directory.split('/')[-2],
                                              output_descriptor,
                                              commands,
                                              output_directory)
    if commands_threads != {} and 'Long operations' in criterias:
        long_actions, needed_linenum, reasons = find_long_operations(
                                                            commands_threads,
                                                            needed_linenum,
                                                            reasons)
    return commands_threads, long_actions, new_commands, command_lvl, \
        needed_linenum, reasons


def link_commands(log_dir, output_descriptor, commands,
                  output_directory):
    without_parents = []
    new_commands = {}
    for command_id in sorted(commands.keys()):
        if 'childs' not in commands[command_id].keys():
            new_commands[command_id] = commands[command_id]
            new_commands[command_id]['lvl'] = 1
        else:
            for child in commands[command_id]['childs']:
                if child['child_id'] not in commands.keys():
                    commands[command_id]['childs'].remove(child)
            if len(commands[command_id]['childs']) == 0:
                commands[command_id].pop('childs', None)
                new_commands[command_id] = commands[command_id]
                new_commands[command_id]['lvl'] = 1
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
                                        'log': commands[parent]['log'],
                                        'first_line_num': commands[parent][
                                                          'first_line_num'],
                                        'id': parent,
                                        'lvl': 2,
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
                            if child['child_id'] in commands.keys():
                                commands.pop(child['child_id'])
                            if child['child_id'] in new_commands.keys():
                                new_commands.pop(child['child_id'])
                break
    heads = []
    while(len(commands) > 0):
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
                                        'log': commands[parent]['log'],
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
                            new_commands[child['child_id']]['lvl'] + 1
                        if child['child_id'] in commands.keys():
                            commands.pop(child['child_id'])
                        if child['child_id'] in new_commands.keys():
                            new_commands.pop(child['child_id'])
            break
    for com in without_parents:
        new_commands[com['id']] = com
    new_commands, command_lvl = change_lvl_numbering(new_commands)
    json.dump(new_commands, open(os.path.join(output_directory,
              log_dir + '_commands.json'), 'w'), indent=4, sort_keys=True)
    return new_commands, command_lvl


def change_lvl_numbering(commands):
    command_lvl = {}
    cur_lvl = 1
    for com in commands.keys():
        commands[com], lvl = change_lvl_numbering_recursive(commands[com],
                                                            cur_lvl,
                                                            command_lvl)
    return commands, command_lvl


def change_lvl_numbering_recursive(com, cur_lvl, lvls):
    com['lvl'] = cur_lvl
    lvls[com['id']] = cur_lvl
    if ('zchildren' not in com.keys()
            and 'ztasks' not in com.keys()):
        return com, lvls
    if 'zchildren' in com.keys():
        for child_id, child in enumerate(com['zchildren']):
            com['zchildren'][child_id], lvl = \
                change_lvl_numbering_recursive(child, cur_lvl + 1, lvls)
            lvls.update(lvl)
    elif 'ztasks' in com.keys():
        for child_id, child in enumerate(com['ztasks']):
            com['ztasks'][child_id], lvl = \
                change_lvl_numbering_recursive(child, cur_lvl + 1, lvls)
            lvls.update(lvl)
    return com, lvls


def find_parent(output_descriptor, commands, command_id):
    parent = None
    for com in sorted(commands.keys()):
        if ('childs' in commands[com].keys() and command_id in
                [c['child_id'] for c in commands[com]['childs']]):
            parent = com
            break
    return parent


def find_vm_tasks_libvirtd(positions, output_descriptor, log_directory,
                           log, file_formats, tz_info, time_range_info,
                           output_directory, needed_linenum, reasons,
                           criterias):
    commands_threads = {}
    long_actions = []
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
    f.seek(0, os.SEEK_END)
    file_len = f.tell()
    widget_style = [log + ':', progressbar.Percentage(), ' (',
                    progressbar.SimpleProgress(), ')', ' ',
                    progressbar.Bar(), ' ', progressbar.Timer()]
    bar = ProgressBar(widgets=widget_style, max_value=file_len)
    for tr_idx, pos in enumerate(positions):
        f.seek(pos, os.SEEK_SET)
        i = pos
        bar.update(i)
        for line_num, line in enumerate(f):
            i += len(line)
            bar.update(i)
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
            start = re.search(r"Thread (.+?) \((.+?)\) is now running " +
                              r"job (.+)", line)
            if (start is not None):
                if (start.group(1) not in commands_threads.keys()):
                    commands_threads[start.group(1)] = []
                commands_threads[start.group(1)] += [
                                 {'command_name': start.group(3),
                                  'command_start_name': start.group(3),
                                  'start_line_num': line_num + 1,
                                  'start_time': dt,
                                  'log': log}]
                continue
            finish = re.search(r"Thread (.+?) \((.+?)\) finished job (.+?)" +
                               r"( .*|$)", line)
            if (finish is not None):
                if (finish.group(1) not in commands_threads.keys()):
                    continue
                else:
                    com_list = [com['command_name'] for com in
                                commands_threads[finish.group(1)]]
                    try:
                        com_id = len(com_list) - 1 - \
                                 com_list[::-1].index(finish.group(3))
                    except:
                        continue
                commands_threads[finish.group(1)][com_id]['finish_time'] = dt
                commands_threads[finish.group(1)][com_id][
                                            'finish_line_num'] = line_num + 1
                if ('start_time' in commands_threads[
                        finish.group(1)][com_id].keys()):
                    commands_threads[finish.group(1)][com_id]['duration'] = \
                        commands_threads[finish.group(1)][
                                         com_id]['finish_time'] -\
                        commands_threads[finish.group(1)][
                                         com_id]['start_time']
                continue
            # qemu monitor
            send_monitor = re.search(r"mon\ *=\ *(.+?)\ +buf\ *\=\ *" +
                                     r"\{\"execute.+\"id\"\:\ *\"(.+?)\"\}",
                                     line)
            if send_monitor is not None:
                if (send_monitor.group(1) not in qemu_monitor.keys()):
                    qemu_monitor[send_monitor.group(1)] = []
                qemu_monitor[send_monitor.group(1)] += [
                                            {'send_time': dt,
                                             'id': send_monitor.group(2),
                                             'start_line_num':
                                             str(line_num + 1),
                                             'log': log}]

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
                            mes_idx]['finish_line_num'] = str(line_num + 1)
                        qemu_monitor[return_monitor.group(1)][
                            mes_idx]['duration'] = duration
                        if ('Long operations' in criterias):
                            needed_linenum.add(log + ':' + str(line_num + 1))
                            if (log + ':' + str(line_num + 1)
                                    not in reasons.keys()):
                                reasons[log + ':' + str(line_num + 1)] = set()
                            reasons[log + ':' + str(line_num + 1)].add(
                                                            'Long monitor')
                            needed_linenum.add(log + ':' + qemu_monitor[
                                                return_monitor.group(1)][
                                                mes_idx]['start_line_num'])
                            if (log + ':' + qemu_monitor[
                                    return_monitor.group(1)][mes_idx][
                                    'start_line_num'] not in reasons.keys()):
                                reasons[log + ':' + qemu_monitor[
                                                return_monitor.group(1)][
                                                mes_idx][
                                                'start_line_num']] = set()
                            reasons[log + ':' + qemu_monitor[
                                                return_monitor.group(1)][
                                                mes_idx][
                                                'start_line_num']].add(
                                                    'Long monitor')
                        break

    f.close()
    bar.finish()
    json.dump(qemu_monitor, open(os.path.join(output_directory,
                                 log_directory.split('/')[-2] +
                                 '_qemu_libvirt.json'),
                                 'w'), indent=4, sort_keys=True)
    # json.dump(commands_threads, open(os.path.join(output_directory,
    #                                  'tasks_libvirtd_' +
    #                                  log_directory.split('/')[-2] +
    #                                  '.json'),
    #                                  'w'), indent=4, sort_keys=True)
    if commands_threads != {} and 'Long operations' in criterias:
        long_actions, needed_linenum, reasons = find_long_operations(
                                                            commands_threads,
                                                            needed_linenum,
                                                            reasons)
    # json.dump(commands_threads, open(os.path.join(output_directory,
    #                                  'filtered_tasks_libvirtd_' +
    #                                  log_directory.split('/')[-2] +
    #                                  '.json'),
    #                                  'w'), indent=4, sort_keys=True)
    return commands_threads, long_actions, needed_linenum, reasons


def find_long_operations(all_threads, needed_linenum, reasons):
    full_operations_time = {}
    operations_time = {}
    long_operations = {}
    for thread in all_threads:
        for command in all_threads[thread]:
            if ('duration' in command.keys()):
                if command['command_start_name'] not in operations_time.keys():
                    operations_time[command['command_start_name']] = []
                operations_time[command['command_start_name']] += [command]
            elif ('duration_full' in command.keys()):
                if command['command_name'] not in full_operations_time.keys():
                    full_operations_time[command['command_name']] = []
                full_operations_time[command['command_name']] += [command]

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
                needed_linenum.add(c_id['log'] + ':' +
                                   str(c_id['start_line_num']))
                needed_linenum.add(c_id['log'] + ':' +
                                   str(c_id['finish_line_num']))
                if (c_id['log'] + ':' + str(c_id['start_line_num'])
                        not in reasons.keys()):
                    reasons[c_id['log'] + ':' +
                            str(c_id['start_line_num'])] = set()
                reasons[c_id['log'] + ':' + str(c_id['start_line_num'])].add(
                    'Task(duration=' + str(np.round(c_id['duration'], 2)) +
                    ')')
                if (c_id['log'] + ':' + str(c_id['finish_line_num'])
                        not in reasons.keys()):
                    reasons[c_id['log'] + ':' +
                            str(c_id['finish_line_num'])] = set()
                reasons[c_id['log'] + ':' + str(c_id['finish_line_num'])].add(
                    'Task(duration=' + str(np.round(c_id['duration'], 2)) +
                    ')')
    for command in sorted(full_operations_time.keys()):
        com_time = [c_id['duration_full']
                    for c_id in full_operations_time[command]]
        med_com_time = np.median(com_time)
        std_com_time = np.std(com_time)
        for c_id in full_operations_time[command]:
            if ((c_id['duration_full'] > med_com_time + 3*std_com_time
                    and c_id['duration_full'] > 1)
                    or c_id['duration_full'] > 5):
                if command not in long_operations.keys():
                    long_operations[command] = []
                long_operations[command] += [c_id['init_time']]
                needed_linenum.add(c_id['log'] + ':' +
                                   str(c_id['init_line_num']))
                needed_linenum.add(c_id['log'] + ':' +
                                   str(c_id['end_line_num']))
                if (c_id['log'] + ':' + str(c_id['init_line_num'])
                        not in reasons.keys()):
                    reasons[c_id['log'] + ':' +
                            str(c_id['init_line_num'])] = set()
                reasons[c_id['log'] + ':' + str(c_id['init_line_num'])].add(
                    'Task(duration=' + str(np.round(c_id[
                        'duration_full'], 2)) + ')')
                if (c_id['log'] + ':' + str(c_id['end_line_num'])
                        not in reasons.keys()):
                    reasons[c_id['log'] + ':' +
                            str(c_id['end_line_num'])] = set()
                reasons[c_id['log'] + ':' + str(c_id['end_line_num'])].add(
                            'Task(duration=' + str(np.round(c_id[
                                'duration_full'], 2)) + ')')
    return long_operations, needed_linenum, reasons
