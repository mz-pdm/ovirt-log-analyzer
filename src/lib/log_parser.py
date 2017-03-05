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
import re
import sys
from time import strptime
from file_read_backwards import FileReadBackwards


class LogParser(object):

    vm_names = {'centos-server-text': '43f02a2d-e563-4f11-a7bc-9ee191cfeba1',
                'debian': 'dd18b0d2-ecd0-43dc-9f86-7e1219780967'}
    engine_log_file = 'engine.log'
    engine_log_address = '../ovirt_logs/' + engine_log_file
    read_engine_log = FileReadBackwards(engine_log_address, encoding="utf-8")
    file_name = ''
    file_address = '../ovirt_logs/' + file_name

    def main(self):
        pass

    def search(self):
        vm_name = sys.argv[1]
        start_time = strptime(sys.argv[2] + ' ' + sys.argv[3], '%Y-%m-%d %H:%M:%S')
        end_time = strptime(sys.argv[4] + ' ' + sys.argv[5], '%Y-%m-%d %H:%M:%S')
        file_name = sys.argv[6]
        self.file_address = '../ovirt_logs/' + file_name

        for line in self.read_engine_log:
            if re.match(r'^[0-9]{4}', line):
                log_time = strptime(line[0:19], '%Y-%m-%d %H:%M:%S')
                if log_time < end_time:
                    if vm_name in line:
                        print(line)

        with open(self.file_address, 'r') as vm_file_log:
            for line in vm_file_log:
                if re.match(r'^[0-9]{4}', line):
                    log_time = strptime(line[0:19],'%Y-%m-%d %H:%M:%S')
                    if start_time<= log_time <= end_time:
                        self.search_for_vm_data(line, vm_name)
                    elif log_time >= end_time:
                        break

    def search_for_vm_data(self, line, vm_name):
        if vm_name in line:
            print(line)

    def get_all_vm_active_by_time(self):
        end_time = strptime(sys.argv[1] + ' ' + sys.argv[2], '%Y-%m-%d %H:%M:%S')
        vm_dict = {}
        for line in self.read_engine_log:
            if re.match(r'^[0-9]{4}', line):
                log_time = strptime(line[0:19], '%Y-%m-%d %H:%M:%S')
                if log_time <= end_time and 'FullListVDSCommand' in line:
                    vm_id_search = re.search(r'vmId=\'[\w|-]*\'',line)
                    if vm_id_search != None:
                        vm_id = vm_id_search.group(0)[6:-1]
                    vm_name_search = re.search(r'vmName=[\w|-]*',line)
                    if vm_name_search != None:
                        vm_name = vm_name_search.group(0)[7:]
                    if vm_name not in vm_dict:
                        vm_dict[vm_name] = vm_id
        return vm_dict

    if __name__ == '__main__':
        main()

