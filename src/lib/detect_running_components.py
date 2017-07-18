import os
import re
import lzma
import json
import pytz
import numpy as np
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
                     time_range_info):
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
                  engine_formats,
                  tz_info,
                  time_range_info,
                  user_vms, user_hosts):
    commands_threads = {}
    needed_threads = set()
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
        firstline = f.readline()
        for fmt in engine_formats:
            prog = re.compile(fmt)
            fields = prog.search(firstline)
            if fields is not None:
                engine_format = prog
        if fields is None:
            # Format is not found
            continue
        f.seek(0, 0)
        for line_num, line in enumerate(f):
            fields = engine_format.search(line)
            if fields is None:
                # Tracebacks will be added anyway
                continue
            fields = fields.groupdict()
            if (time_range_info != [] and not
                    parse_date_time(fields["date_time"], tz_info[log],
                                    time_range_info)):
                continue
            if (any([vm in fields["message"] for vm in user_vms]) or
                    any([host in fields["message"] for host in user_hosts])):
                needed_threads.add(fields["thread"])

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
                                        'command_start_name'] = \
                            finish.group(2)
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
                        commands_threads[finish.group(1)][
                                         com_id]['duration'] = \
                            commands_threads[finish.group(1)][
                                         com_id]['finish_time'] -\
                            commands_threads[finish.group(1)][
                                         com_id]['start_time']
        f.close()
    json.dump(commands_threads, open('tasks_' +
                                     log_directory.split('/')[-2] +
                                     '.json',
                                     'w'), indent=4, sort_keys=True)
    commands_threads, long_actions = select_needed_commands(commands_threads,
                                                            needed_threads)
    json.dump(commands_threads, open('filetredtasks_' +
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
            if ('duration' in command):
                if command['command_start_name'] not in \
                        operations_time.keys():
                    operations_time[command['command_start_name']] = []
                operations_time[command['command_start_name']] += \
                    [{'duration': command['duration'], 'start_time':
                     command['start_time']}]

    commands_ex_time = [t['duration'] for c in operations_time
                        for t in operations_time[c]]
    ex_median = np.median(commands_ex_time)
    ex_std = np.std(commands_ex_time)

    for command in sorted(operations_time.keys()):
        com_time = [c_id['duration'] for c_id in operations_time[command]]
        med_com_time = np.median(com_time)
        std_com_time = np.std(com_time)
        for c_id in operations_time[command]:
            if ((c_id['duration'] > med_com_time + 3*std_com_time) or
                    (c_id['duration'] > ex_median + 3*ex_std)):
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
                if existed_commands['command_start_name'] == \
                        command['command_start_name'] and \
                        existed_commands['start_time'] == \
                        command['start_time']:
                    found = True
            if found:
                continue
            vm_threads[thread] += [command]
    return vm_threads, long_operations
