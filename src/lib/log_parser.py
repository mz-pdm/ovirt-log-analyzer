import re
from time import strptime, strftime
from file_read_backwards import FileReadBackwards


class LogParser(object):
    engine_log_file = 'engine.log'
    engine_log_address = '../ovirt_logs/' + engine_log_file
    read_engine_log = FileReadBackwards(engine_log_address, encoding="utf-8")
    file_name = ''
    file_address = '../ovirt_logs/' + file_name

    def search(self):
        self.file_address = '../ovirt_logs/' + self.file_name
        vm_names = self.get_all_vm_active()
        vm_id = vm_names[self.vm_name]

        for line in self.read_engine_log:
            if re.match(r'^[0-9]{4}', line):
                log_time = strptime(line[0:19], '%Y-%m-%d %H:%M:%S')
                if log_time < self.end_time and vm_id in line:
                        self.search_for_vm_data_in_engine_log(line, log_time)
        try:
            with open(self.file_address, 'r') as vm_file_log:
                for line in vm_file_log:
                    if re.match(r'^[0-9]{4}', line):
                        log_time = strptime(line[0:19],'%Y-%m-%d %H:%M:%S')
                        if self.start_time<= log_time <= self.end_time and vm_id in line:
                            self.search_for_vm_data_in_vm_log(line, log_time)
                        elif log_time >= self.end_time:
                            break
        except FileNotFoundError:
            print('Log file %s not found' % self.file_name)

    def search_for_vm_data_in_vm_log(self, line, log_time):
        print_log_time = strftime('%Y-%m-%d %H:%M:%S', log_time)
        if ' WARN ' in line:
            print(print_log_time, re.search(r'WARN.+', line).group(0))
        elif ' ERROR ' in line:
            print(print_log_time, re.search(r'ERROR.+', line).group(0))
        elif ' INFO ' in line:
            if len(line) <= 300:
                print(print_log_time, re.search(r'INFO.+', line).group(0))
            elif 'START' in line:
                print(print_log_time,
                      'INFO',
                      re.search(r'START \S+', line).group(0))
            elif ' FINISH ' in line:
                print(print_log_time,
                      'INFO',
                      re.search(r'FINISH \S+', line).group(0))

    def search_for_vm_data_in_engine_log(self, line, log_time):
        print_log_time = strftime('%Y-%m-%d %H:%M:%S', log_time)
        logged_actions = [
            'moved.*',              # VM moved from one state to another
            'Running command.*'     # Command is run on VM
        ]
        for command in logged_actions:
            if re.search(command, line):
                print(print_log_time, re.search(command, line).group(0))
            elif re.search(r'.+FullListVDSCommand.+FINISH.+', line):
                print(print_log_time,
                      'VM state:',
                      re.search(r'emulatedMachine=\S+', line).group(0),
                      re.search(r'vmType=\S+', line).group(0),
                      re.search(r'memSize=\S+', line).group(0),
                      re.search(r'status=\S+', line).group(0),
                      re.search(r'displayNetwork=\S+', line).group(0),
                      sep='\n\t')
            elif re.search(r'.+ ERROR .+', line):
                if 'VmsMonitoring' in line:
                    print(print_log_time,
                          'ERROR VmsMonitoring',
                          re.search(r'Rerun.+ ', line).group(0))
                else:
                    print(print_log_time,
                          'ERROR',
                          re.search(r'\'\w+', line).group(0).replace('\'', ''),
                          re.search(r'execution failed:.+', line).group(0))

    def get_all_vm_active(self):
        vm_dict = {}
        for line in self.read_engine_log:
            if re.match(r'^[0-9]{4}', line):
                log_time = strptime(line[0:19], '%Y-%m-%d %H:%M:%S')
                if log_time <= self.end_time and 'FullListVDSCommand' in line:
                    vm_id_search = re.search(r'vmId=\'[\w|-]*\'',line)
                    if vm_id_search:
                        vm_id = vm_id_search.group(0)[6:-1]
                    vm_name_search = re.search(r'vmName=[\w|-]*',line)
                    if vm_name_search:
                        vm_name = vm_name_search.group(0)[7:]
                    if vm_name not in vm_dict:
                        vm_dict[vm_name] = vm_id
        return vm_dict

    def __init__(self, vm_name, start_time, end_time, file_name):
        self.vm_name = vm_name
        self.start_time = start_time
        self.end_time = end_time
        self.file_name = file_name
