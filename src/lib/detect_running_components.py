import os
import re
import lzma
import json
import pytz
from datetime import datetime


def parse_date_time(line, time_zone, time_ranges):
    dt = re.findall(r"[0-9\-]{10}[\sT][0-9]{2}:[0-9]{2}:" +
                    r"[0-9]{2}[\.\,0-9]*[\+\-0-9Z]*",
                    line)
    if len(dt) == 0:
        return 0
    dt = dt[0]
    dt = dt.replace('T', ' ')
    dt = dt.replace('Z', '+0000')
    dt = dt.replace('.', ',')
    time_part = dt.partition(' ')[2]
    # for "2017-05-12 03:23:31,135-04" format
    if (('+' in time_part and
         len(time_part.partition('+')[2]) < 4) or
        ('-' in time_part and
         len(time_part.partition('-')[2]) < 4)):
        dt += '00'
    elif not ('+' in time_part or '-' in time_part):
        # if we have time without time zone
        dt += time_zone
    dt_formats = ["%Y-%m-%d %H:%M:%S,%f%z",
                  "%Y-%m-%d %H:%M:%S%z"]
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
    # Check user-defined time range
    if time_ranges != [] and not any([date_time >= tr[0] and
       date_time <= tr[1]
       for tr in time_ranges]):
        return 0
    return date_time


def find_all_vm_host(output_descriptor,
                     log_directory,
                     files,
                     tz_info,
                     time_range_info,
                     tasks):
    vm_names = {}
    host_names = {}
    for log in files:
        full_filename = os.path.join(log_directory, log)
        if log[-4:] == '.log':
                f = open(full_filename)
        elif log[-3:] == '.xz':
            f = lzma.open(full_filename, 'rt')
        else:
            output_descriptor.write("Unknown file extension: %s" % log)
            continue
        for line_num, line in enumerate(f):
            if (time_range_info != [] and
                not parse_date_time(line, tz_info[log],
                                    time_range_info)):
                continue
            if any([v in line for v in ['vmId', 'vmName', 'VM_NAME']]):
                if (re.search(r"vmId=\'(.+?)\'", line) is not None):
                    res_id = re.search(r"vmId=\'(.+?)\'", line).group(1)
                elif (re.search(r"\'vmId\'\ *[:=]\ *u*\'(.+?)\'",
                                line) is not None):
                    res_id = re.search(
                        r"\'vmId\'\ *[:=]\ *u*\'(.+?)\'", line).group(1)
                else:
                    res_id = '<id_not_found>'

                if (re.search(r"vmName\ *=\ *(.+?),", line) is not None):
                    res_name = re.search(r"vmName\ *=\ *(.+?),",
                                         line).group(1)
                elif (re.search(r"vm\ *=\ *\'VM\ *\[([^\[\]]*?)\]\'",
                                line) is not None):
                    res_name = re.search(r"vm\ *=\ *\'VM\ *\[([^\[\]]*?)\]\'",
                                         line).group(1)
                elif (re.search(r"\[(.+?)=VM_NAME\]", line) is not None):
                    res_name = re.search(r"\[([^\[\]]*?)=VM_NAME\]",
                                         line).group(1)
                elif (re.search(r"\[(.+?)=VM\]", line) is not None):
                    res_name = re.search(r"\[([^\[\]]*?)=VM\]",
                                         line).group(1)
                elif (re.search(r"\'vmName\'\ *[:=]\ *u*\'([^\']*?)\'",
                                line) is not None):
                    res_name = re.search(r"\'vmName\'" +
                                         r"\ *[:=]\ *u*\'([^\']*?)\'",
                                         line).group(1)
                else:
                    res_name = '<name_not_found>'

                if res_name not in vm_names.keys():
                    vm_names[res_name] = []
                if res_id not in vm_names[res_name]:
                    if vm_names[res_name] != [] and res_id == '<id_not_found>':
                        pass

                    elif res_id != '<id_not_found>' and \
                            '<id_not_found>' in vm_names[res_name]:
                        vm_names[res_name].remove('<id_not_found>')
                        vm_names[res_name] += [res_id]

                    else:
                        vm_names[res_name] += [res_id]

            if 'hostId' in line and 'HostName' in line:
                res_name = re.search(r"HostName\ *=\ *(.+?),", line)
                if res_name is not None:
                    res_name = res_name.group(1)
                else:
                    res_name = '<name_not_found>'
                res_id = re.search(r"hostId=\'(.+?)\'", line)
                if res_id is not None:
                    res_id = res_id.group(1)
                else:
                    res_id = '<id_not_found>'

                if res_name not in host_names.keys():
                    host_names[res_name] = []
                if res_id not in host_names[res_name]:
                    host_names[res_name] += [res_id]

            other_vm = re.search(r'VM\ *\'(.{30,40}?)\'\ *' +
                                 r'\(([^\(\)]*?)\)', line)
            if other_vm is not None:
                if other_vm.group(2) not in vm_names.keys():
                    vm_names[other_vm.group(2)] = []
                if other_vm.group(1) not in vm_names[other_vm.group(2)]:
                    if vm_names[other_vm.group(2)] != [] and \
                            other_vm.group(1) == '<id_not_found>':
                        pass
                    elif other_vm.group(1) != '<id_not_found>' and \
                            '<id_not_found>' in vm_names[other_vm.group(2)]:
                        vm_names[other_vm.group(2)].remove('<id_not_found>')
                        vm_names[other_vm.group(2)] += [other_vm.group(1)]
                    else:
                        vm_names[other_vm.group(2)] += [other_vm.group(1)]
        f.close()
    if '<name_not_found>' in vm_names.keys():
        for name in vm_names.keys():
            if name == '<name_not_found>':
                continue
            for uuid in vm_names[name]:
                if uuid in vm_names['<name_not_found>']:
                    vm_names['<name_not_found>'].remove(uuid)
        if vm_names['<name_not_found>'] == []:
            vm_names.pop('<name_not_found>', None)
    return vm_names, host_names


def find_vm_tasks(output_descriptor,
                  log_directory,
                  files,
                  tz_info,
                  time_range_info):
    commands_threads = {}
    for log in files:
        if 'engine' not in log:
            continue
        full_filename = os.path.join(log_directory, log)
        if (log[-4:] == '.log'):
                f = open(full_filename)
        elif (log[-3:] == '.xz'):
            f = lzma.open(full_filename, 'rt')
        else:
            output_descriptor.write("Unknown file extension: %s" % log)
            continue
        for line_num, line in enumerate(f):
            if (time_range_info != [] and not
                    parse_date_time(line, tz_info[log], time_range_info)):
                continue
            com = re.search(r"\((.+?)\)\ +\[.*?\]\ +Running command:\ +" +
                            r"([^\s]+)Command", line)
            if (com is not None):
                if (com.group(1) not in commands_threads.keys()):
                    commands_threads[com.group(1)] = []
                commands_threads[com.group(1)] += [
                                 {'command_name': com.group(2)}]

            start = re.search(r"\((.+?)\)\ +\[.*?\]\ +START,\ +" +
                              r"([^\s]+)Command.*\ +log id:\ (.+)", line)
            if (start is not None):
                if (start.group(1) not in commands_threads.keys()):
                    commands_threads[start.group(1)] = [
                                     {'command_name': start.group(2)}]
                    com_id = 0
                else:
                    com_list = [com['command_name'] for com in
                                commands_threads[start.group(1)]]
                    try:
                        com_id = com_list.index(start.group(2))
                    except:
                        commands_threads[start.group(1)] += [
                                         {'command_name': start.group(2)}]
                        com_id = -1
                commands_threads[start.group(1)][com_id][
                                 'command_start_name'] = start.group(2)
                commands_threads[start.group(1)][
                                 com_id]['start_time'] = \
                    parse_date_time(line, tz_info, time_range_info)
                commands_threads[start.group(1)][com_id][
                                 'log_id'] = start.group(3)

            finish = re.search(r"\((.+?)\)\ +\[.*?\]\ +FINISH,\ +" +
                               r"([^\s]+)Command.*\ +log id:\ (.+)", line)
            if (finish is not None):
                if (finish.group(1) not in commands_threads.keys()):
                    commands_threads[finish.group(1)] = [
                                     {'command_name': finish.group(2)}]
                    com_id = 0
                    commands_threads[finish.group(1)][com_id][
                                     'command_start_name'] = finish.group(2)
                    commands_threads[finish.group(1)][com_id][
                                     'log_id'] = finish.group(3)
                else:
                    com_list = [com['command_name'] for com in
                                commands_threads[finish.group(1)]]
                    try:
                        com_id = com_list.index(finish.group(2))
                    except:
                        commands_threads[finish.group(1)] += [{
                                         'command_name': finish.group(2)}]
                        com_id = -1
                        commands_threads[finish.group(1)][com_id][
                                        'command_start_name'] = finish.group(2)
                        commands_threads[finish.group(1)][com_id][
                                        'log_id'] = finish.group(3)
                for task in commands_threads[finish.group(1)]:
                    if ('command_start_name' in task.keys() and
                            task['command_start_name'] == finish.group(2) and
                            task['log_id'] == finish.group(3)):
                        commands_threads[finish.group(1)][
                                         com_id]['finish_time'] = \
                            parse_date_time(line,
                                            tz_info,
                                            time_range_info)
        f.close()
    json.dump(commands_threads, open('tasks_' +
                                     log_directory.split('/')[-2] +
                                     '.json',
                                     'w'), indent=4, sort_keys=True)
    return commands_threads
