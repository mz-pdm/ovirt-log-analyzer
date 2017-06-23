import os
import re
import pytz
from datetime import datetime

def parse_date_time(line, time_zone, time_ranges):
    dt = re.findall(
            r"[0-9\-]{10}[\sT][0-9]{2}:[0-9]{2}:[0-9]{2}[\.\,0-9]*[\+\-0-9Z]*",\
            line)
    if len(dt) == 0:
        return 0
    dt = dt[0]
    dt = dt.replace('T', ' ')
    dt = dt.replace('Z', '+0000')
    dt = dt.replace('.', ',')
    time_part = dt.partition(' ')[2]
    #for "2017-05-12 03:23:31,135-04" format
    if ('+' in time_part and len(time_part.partition('+')[2]) < 4) \
        or ('-' in time_part and \
            len(time_part.partition('-')[2]) < 4):
        dt += '00'
    elif not ('+' in time_part or '-' in time_part):
        #if we have time without time zone
        dt += time_zone
    dt_formats = ["%Y-%m-%d %H:%M:%S,%f%z", \
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
    #Check user-defined time range
    if not any([date_time >= tr[0] and \
                date_time <= tr[1] \
                for tr in time_ranges]):
        return 0
    return date_time

def find_all_vm_host(output_descriptor,
                        log_directory,
                        files,
                        tz_info,
                        time_range_info):
    vm_ids = {}
    host_ids = {}

    for log in files:
        full_filename = os.path.join(log_directory, log) + '.log'
        with open(full_filename) as f:
            for line_num, line in enumerate(f):
                if time_range_info != [] and not \
                    parse_date_time(line, tz_info[log], time_range_info):
                    continue
                if 'vmId' in line:
                    res_name = re.findall(r"vmName\ *=\ *(.+?),", line)
                    if len(res_name) == 0:
                        res_name = re.findall(r"vm\ *=\ *'VM\ *\[(.+?)\]'", line)
                    res_id = re.findall(r"vmId=\'(.+?)\'", line)
                    if len(res_name) == 1 and len(res_id) == 1:
                        vm_ids[res_id[0]] = res_name[0]
                if 'hostId' in line and 'HostName' in line:
                    res_name = re.findall(r"HostName\ *=\ *(.+?),", line)
                    res_id = re.findall(r"hostId=\'(.+?)\'", line)
                    if len(res_name) == 1 and len(res_id) == 1:
                        host_ids[res_id[0]] = res_name[0]
    print(vm_ids)
    return vm_ids, host_ids
