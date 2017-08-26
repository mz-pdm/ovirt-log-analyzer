import os
import re
import lzma
import progressbar
from multiprocessing import Manager, Pool
from lib.create_error_definition import loop_over_lines
from lib.errors_statistics import merge_all_errors_by_time, \
                                  clusterize_messages
from lib.represent_statistics import print_only_dt_message
from lib.detect_running_components import find_vm_tasks_engine, \
                                          find_vm_tasks_libvirtd, \
                                          find_all_vm_host, \
                                          find_time_range, \
                                          find_needed_linenum
from lib.ProgressPool import ProgressPool
from progressbar import ProgressBar


class LogAnalyzer:
    # out_descr
    # directory
    # filenames[]
    # time_zones[]
    # formats_templates[]
    # ----
    # found_logs['log1',...]
    # log_file_format{"log1":...,}
    # all_errors{"log1":...,}
    # format_fields{'log1':...}
    def __init__(self, out_descr, directory, filenames, tz, criterias,
                 time_ranges, user_vms, user_events, user_hosts,
                 templates_filename, additive_link, output_dir):
        self.out_descr = out_descr
        self.directory = directory
        self.output_dir = output_dir
        self.time_ranges = time_ranges
        self.user_vms = user_vms
        self.user_events = user_events
        self.user_hosts = user_hosts
        self.additive_link = additive_link
        if len(criterias) == 1 and criterias[0] == 'All':
            self.criterias = ['VM id', 'Host id', 'Event', 'Subtasks',
                              'Error or warning',
                              'Differ by VM ID', 'Exclude frequent messages',
                              'Coverage', 'Increased errors',
                              'Long operations']
        else:
            self.criterias = criterias
        # parse formats file
        formats = open(templates_filename, 'r').read().split('\n')
        self.formats_templates = []
        format_num = 0
        for line in formats:
            if line[0] == '@':
                format_name = line[1:]
            elif line[0:2] == 'r^' and format_name != '':
                try:
                    re.compile(line[1:])
                except:
                    self.out_descr.write("Wrong format of regexp: %s\n" %
                                         line[1:])
                    exit()
                self.formats_templates += [{'name': format_name,
                                            'regexp': line[1:]}]
                format_name = ''
                format_num += 1
            else:
                self.out_descr.write("Wrong format of template: %s\n" % line)
        self.found_logs = []
        self.log_files_format = []
        self.time_zones = []
        for log in filenames:
            full_filename = os.path.join(self.directory, log)
            if not os.path.isfile(full_filename):
                self.out_descr.write("File not found: %s\n" % log)
                continue
            # save log's time zome
            self.time_zones += [tz[log]]
            # find format of a log
            if log[-4:] == '.log':
                f = open(full_filename)
                line = f.readline()
                f.close()
            elif log[-3:] == '.xz':
                f = lzma.open(full_filename, 'rt')
                line = f.readline()
                f.close()
            else:
                self.out_descr.write("Unknown file extension: %s" % log)
                continue
            # save name of actually opened logfile
            self.found_logs += [log]
            for file_format_num in range(len(self.formats_templates)):
                prog = re.compile(self.formats_templates[file_format_num][
                                                    'regexp'])
                result = prog.search(line)
                if result is not None:
                    self.log_files_format += [prog]
                    break
        if (self.found_logs == []):
            out_descr.write('No logfiles found.\n')
            exit()

    def read_time_ranges(self):
        self.positions = find_needed_linenum(self.out_descr,
                                             self.directory,
                                             self.found_logs,
                                             self.time_zones,
                                             self.time_ranges)
        self.total_time_ranges, self.found_logs = \
            find_time_range(self.out_descr, self.directory,
                            self.found_logs, self.time_zones,
                            self.time_ranges)
        if (self.found_logs != [] and self.time_ranges == []):
            max_time = max([t for l in self.total_time_ranges.keys()
                            for t in self.total_time_ranges[l]])
            min_time = min([t for l in self.total_time_ranges.keys()
                            for t in self.total_time_ranges[l]])
            self.time_ranges = [[min_time, max_time]]

    def find_vms_and_hosts(self):
        self.all_vms, self.all_hosts, self.not_running_vms, \
        self.not_found_vmnames, self.not_found_hostnames, \
        self.positions, vm_timeline =  find_all_vm_host(self.positions,
                                                        self.out_descr,
                                                        self.output_dir,
                                                        self.directory,
                                                        self.found_logs,
                                                        self.time_zones,
                                                        self.time_ranges)
        if self.user_vms == []:
            for k in self.all_vms.keys():
                self.user_vms += [k]
                for i in self.all_vms[k]['id']:
                    self.user_vms += [i]
            self.user_vms += self.not_running_vms
            self.user_vms += self.not_found_vmnames
        else:
            for user_vm in self.user_vms.copy():
                for vm_name in self.all_vms.keys():
                    for vm_id in self.all_vms[vm_name]['id']:
                        if user_vm == vm_name:
                            self.user_vms += [vm_id]
                            break
                        elif user_vm == vm_id:
                            self.user_vms += [vm_name]
                            break
                        else:
                            pass
        if self.user_hosts == []:
            for k in self.all_hosts.keys():
                self.user_hosts += [k]
                for i in self.all_hosts[k]['id']:
                    self.user_hosts += [i]
            self.user_hosts += self.not_found_hostnames
        else:
            for user_host in self.user_hosts.copy():
                for host_name in self.all_hosts.keys():
                    for host_id in self.all_hosts[host_name]['id']:
                        if user_host == host_name:
                            self.user_hosts += [host_id]
                        elif user_host == host_id:
                            self.user_hosts += [host_name]
                        else:
                            pass
        self.user_vms = list(set(self.user_vms))
        self.user_hosts = list(set(self.user_hosts))
        names = list(vm_timeline.keys())
        for vm_name in names:
            if vm_name not in self.user_vms:
                vm_timeline.pop(vm_name)
                continue
            names2 = list(vm_timeline[vm_name].keys())
            for host_name in names2:
                if host_name not in self.user_hosts:
                    vm_timeline[vm_name].pop(host_name)
        self.vm_timeline = vm_timeline

    def find_vm_tasks(self):
        self.needed_lines = set()
        self.reasons = {}
        engine_formats = [fmt['regexp'] for fmt in self.formats_templates if
                          'engine' in fmt['name']]
        libvirtd_formats = [fmt['regexp'] for fmt in self.formats_templates if
                            'libvirt' in fmt['name']]
        self.vm_tasks = {}
        self.long_tasks = {}
        self.subtasks = {}
        self.stuctured_commands = {}
        for idx, log in enumerate(self.found_logs):
            if 'engine' in log.lower():
                tasks_file, long_tasks_file, self.stuctured_commands[log], \
                    subtasks, cur_needed_lines, cur_reasons = \
                               find_vm_tasks_engine(self.positions[log],
                                                    self.out_descr,
                                                    self.directory,
                                                    log,
                                                    engine_formats,
                                                    self.time_zones[idx],
                                                    self.time_ranges,
                                                    self.output_dir,
                                                    self.needed_lines,
                                                    self.reasons,
                                                    self.criterias)
                self.vm_tasks[log] = tasks_file
                self.long_tasks[log] = long_tasks_file
                self.subtasks.update(subtasks)
                self.needed_lines = self.needed_lines.union(cur_needed_lines)
                self.reasons.update(cur_reasons)
            elif 'libvirt' in log.lower():
                tasks_file, long_tasks_file, cur_needed_lines, \
                cur_reasons = \
                    find_vm_tasks_libvirtd(self.positions[log],
                                           self.out_descr,
                                           self.directory,
                                           log,
                                           libvirtd_formats,
                                           self.time_zones[idx],
                                           self.time_ranges,
                                           self.output_dir,
                                           self.needed_lines,
                                           self.reasons,
                                           self.criterias)
                self.vm_tasks[log] = tasks_file
                self.long_tasks[log] = long_tasks_file
                self.needed_lines = self.needed_lines.union(cur_needed_lines)
                self.reasons.update(cur_reasons)

    def load_data(self, show_warnings, show_progressbar):
        self.all_errors = {}
        self.format_fields = {}
        m = Manager()
        q = m.Queue()
        idxs = range(len(self.found_logs))
        if show_progressbar:
            result = ProgressPool([(process_files,
                                    "{}".format(self.found_logs[i]),
                                    [i, self.found_logs,
                                     self.log_files_format,
                                     self.directory,
                                     self.time_zones,
                                     self.positions,
                                     q,
                                     self.additive_link,
                                     self.user_events,
                                     self.user_hosts,
                                     self.time_ranges,
                                     self.user_vms,
                                     self.vm_timeline,
                                     self.subtasks,
                                     self.needed_lines,
                                     [mes['flow_id']
                                      for l in self.vm_tasks.keys()
                                      for t in self.vm_tasks[l].keys()
                                      for mes in (self.vm_tasks)[l][t]
                                      if ('flow_id' in mes.keys()
                                          and mes['flow_id'] != '')],
                                     show_warnings])
                                   for i in idxs], processes=4)
        else:
            result = []
            run_args = [[i, self.found_logs,
                         self.log_files_format,
                         self.directory,
                         self.time_zones,
                         self.positions,
                         q,
                         self.additive_link,
                         self.user_events,
                         self.user_hosts,
                         self.time_ranges,
                         self.user_vms,
                         self.vm_timeline,
                         self.subtasks,
                         self.needed_lines,
                         [mes['flow_id'] for l in self.vm_tasks.keys()
                          for t in self.vm_tasks[l].keys()
                          for mes in (self.vm_tasks)[l][t] if ('flow_id'
                          in mes.keys() and mes['flow_id'] != '')],
                         show_warnings] for i in idxs]
            widget_style = ['Load: ', progressbar.Percentage(), ' (',
                            progressbar.SimpleProgress(), ')', ' ',
                            progressbar.Bar(), ' ', progressbar.Timer(), ' ',
                            progressbar.AdaptiveETA()]
            bar = ProgressBar(widgets=widget_style)
            with Pool(processes=4) as pool:
                worker = pool.imap(star, run_args)
                for _ in bar(run_args):
                    result += [worker.next()]
        for idx, log in enumerate(self.found_logs):
            self.all_errors[log] = result[idx][0]
            # saving logfile format fields names
            self.format_fields[log] = result[idx][1]
        while not q.empty():
            warn = q.get()
            self.out_descr.write(warn)
        if (self.all_errors == {} or all([self.all_errors[l] == []
                                          for l in self.all_errors.keys()])):
            self.out_descr.write('No matches.\n')
            exit()

    def merge_all_messages(self):
        self.timeline, self.merged_errors, self.all_fields = \
            merge_all_errors_by_time(self.all_errors, self.format_fields)
        try:
            del self.all_errors
        except:
            pass

    def find_important_events(self):
        important_events, new_fields = \
            clusterize_messages(self.out_descr, self.merged_errors,
                                self.all_fields, self.user_events,
                                self.user_vms, self.user_hosts, self.subtasks,
                                self.directory, self.timeline, self.vm_tasks,
                                self.long_tasks, self.output_dir,
                                self.reasons, self.needed_lines,
                                self.criterias, self.vm_timeline)
        return important_events, new_fields

    def print_errors(self, errors_list, new_fields, out):
        # print_all_headers(errors_list, self.list_headers,
        #                   self.format_fields, out)
        print_only_dt_message(errors_list, new_fields, out)


def star(input):
    return process_files(*input)


def process_files(idx, log, formats_templates, directory, time_zones,
                  positions, out_descr, additive, user_events, user_hosts,
                  time_ranges, user_vms, vm_timeline, tasks,
                  needed_lines, flow_ids, show_warnings, progressbar=None,
                  text_header=None):
    if text_header:
        text_header.update_mapping(type_op="Parsing:")
    # gathering all information about errors from a logfile into lists
    lines_info, fields_names = loop_over_lines(directory,
                                               log[idx],
                                               formats_templates[idx],
                                               time_zones[idx],
                                               positions[log[idx]],
                                               out_descr,
                                               additive,
                                               user_events,
                                               user_hosts,
                                               time_ranges,
                                               user_vms,
                                               vm_timeline,
                                               tasks,
                                               needed_lines,
                                               flow_ids,
                                               show_warnings,
                                               progressbar)
    return lines_info, fields_names
