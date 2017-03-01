'''
INPUT TYPE : command line
INPUT:
the name or id of a virtual machine
starting time
end time
log file name

OUTPUT TYPE: standart output
OUTPUT:
important events related to the virtual machine (action: start, stop, migrate, error ...)
final state of the virtual machine at the end time (memory)
'''

from os import path
from time import strptime
import re

vm_names = {'centos-server-text':'43f02a2d-e563-4f11-a7bc-9ee191cfeba1',
           'debian':'dd18b0d2-ecd0-43dc-9f86-7e1219780967'}

# ex1: centos-server-text 2017-02-23 11:01:04 2017-02-23 11:17:53 vdsm-1.log
# ex2: dd18b0d2-ecd0-43dc-9f86-7e1219780967 2017-02-23 10:07:33 2017-02-23 10:10:01 vdsm-2.log

def get_vm_name_and_id(vm):
    for key, value in vm_names.items():
        if vm == key or vm == value:
            vm_name = key
            vm_id = value
            break
    return vm_name, vm_id

def search(vm_name, start_time, end_time, file_address):
    vm_id = vm_names[vm_name]

    with open(engine_log_address, 'r') as vm_engine_log:
        for line in vm_engine_log:
            if re.match(r'^[0-9]{4}', line) != None:
                log_time = strptime(line[0:19],'%Y-%m-%d %H:%M:%S')
                if (log_time >= start_time and log_time <= end_time) and (vm_name in line or vm_id in line):
                    print(line)

    with open(file_address, 'r') as vm_file_log:
        for line in vm_file_log:
            if re.match(r'^[0-9]{4}', line) != None:
                log_time = strptime(line[0:19],'%Y-%m-%d %H:%M:%S')
                if (log_time >= start_time and log_time <= end_time) and (vm_name in line or vm_id in line):
                    print(line)

input_data = input().split()
vm = input_data[0]
vm_name, vm_id = get_vm_name_and_id(vm)
start_time = strptime((input_data[1] + ' ' + input_data[2]), '%Y-%m-%d %H:%M:%S')
end_time = strptime((input_data[3] + ' ' + input_data[4]), '%Y-%m-%d %H:%M:%S')
file_name = input_data[5]
file_address = path.join('.', 'ovirt_logs', file_name)
engine_log = 'engine.log'
engine_log_address = path.join('.', 'ovirt_logs', engine_log)

search(vm_name, start_time, end_time, file_address)