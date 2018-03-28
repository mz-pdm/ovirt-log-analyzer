import os
import re
import progressbar
import pickle
import multiprocessing
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
from lib.util import open_log_file


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
            self.criterias = ['Subtasks',
                              'Error or warning',
                              'Differ by VM ID', 'Exclude frequent messages',
                              'Increased errors',
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
        self.log_files_format = {}
        self.time_zones = []
        for log in filenames:
            if not os.path.isfile(log):
                self.out_descr.write("%s\n" % log)
                self.out_descr.write("File not found: %s\n" % log)
                continue
            # save log's time zome
            self.time_zones += [tz[log]]
            # find format of a log
            f = open_log_file(log)
            if f is None:
                self.out_descr.write("Unknown file extension: %s" % log)
                continue
            line = f.readline()
            f.close()
            # save name of actually opened logfile
            for file_format_num in range(len(self.formats_templates)):
                prog = re.compile(self.formats_templates[file_format_num][
                                                    'regexp'])
                result = prog.search(line)
                if result is not None:
                    self.log_files_format[log] = prog
                    break
            if result is None:
                self.out_descr.write("Unrecognized file format: %s\n" % log)
                continue
            self.found_logs.append(log)
        if (self.found_logs == []):
            out_descr.write('No logfiles found.\n')
            exit()

    def read_time_ranges(self, re_load):
        if (not re_load and os.path.isdir(
                os.path.join(self.directory, 'log_analyzer_cache'))
                and 'time_ranges.pckl' in os.listdir(
                os.path.join(self.directory, 'log_analyzer_cache'))):
            with open(os.path.join(self.directory, 'log_analyzer_cache',
                                   'time_ranges.pckl'), 'rb') as f:
                self.positions, self.total_time_ranges, cur_time_ranges, \
                    self.found_logs = pickle.load(f)
                if self.time_ranges != cur_time_ranges:
                    self.out_descr.write("Warning: time range differs " +
                                         "from the saved version, it can " +
                                         "cause errors or cutting the " +
                                         "result to saved time range. " +
                                         "Didn't you forget to add " +
                                         "--reload flag?\n")
                    self.time_ranges = cur_time_ranges
            return
        if not os.path.isdir(os.path.join(self.directory,
                                          'log_analyzer_cache')):
            os.mkdir(os.path.join(self.directory, 'log_analyzer_cache'))
        self.total_time_ranges, self.found_logs = \
            find_time_range(self.out_descr, self.directory,
                            self.found_logs, self.time_zones,
                            self.time_ranges)
        self.positions = find_needed_linenum(self.out_descr,
                                             self.directory,
                                             self.found_logs,
                                             self.time_zones,
                                             self.time_ranges)
        if (self.found_logs != [] and self.time_ranges == []):
            max_time = max([t for l in self.total_time_ranges.keys()
                            for t in self.total_time_ranges[l]])
            min_time = min([t for l in self.total_time_ranges.keys()
                            for t in self.total_time_ranges[l]])
            self.time_ranges = [[min_time, max_time]]
        with open(os.path.join(self.directory, 'log_analyzer_cache',
                               'time_ranges.pckl'), 'wb') as f:
            pickle.dump([self.positions, self.total_time_ranges,
                         self.time_ranges, self.found_logs], f)

    def find_vms_and_hosts(self, re_load):
        if (not re_load and os.path.isdir(
                os.path.join(self.directory, 'log_analyzer_cache'))
                and 'vms_and_hosts.pckl' in os.listdir(
                os.path.join(self.directory, 'log_analyzer_cache'))):
            with open(os.path.join(self.directory, 'log_analyzer_cache',
                                   'vms_and_hosts.pckl'), 'rb') as f:
                self.all_vms, self.all_hosts, self.not_running_vms, \
                    self.not_found_vmnames, self.not_found_hostnames, \
                    self.positions, self.user_vms, self.user_hosts, \
                    self.vm_timeline = pickle.load(f)
            return
        if not os.path.isdir(os.path.join(self.directory,
                                          'log_analyzer_cache')):
            os.mkdir(os.path.join(self.directory, 'log_analyzer_cache'))
        self.all_vms, self.all_hosts, self.not_running_vms, \
            self.not_found_vmnames, self.not_found_hostnames, \
            self.positions, vm_timeline = find_all_vm_host(self.positions,
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
        with open(os.path.join(self.directory, 'log_analyzer_cache',
                               'vms_and_hosts.pckl'), 'wb') as f:
            pickle.dump([self.all_vms, self.all_hosts, self.not_running_vms,
                         self.not_found_vmnames, self.not_found_hostnames,
                         self.positions, self.user_vms, self.user_hosts,
                         self.vm_timeline], f)

    def find_vm_tasks(self, re_load):
        if (not re_load and os.path.isdir(
                os.path.join(self.directory, 'log_analyzer_cache'))
                and 'vm_tasks.pckl' in
                os.listdir(os.path.join(self.directory,
                                        'log_analyzer_cache'))):
            with open(os.path.join(self.directory, 'log_analyzer_cache',
                                   'vm_tasks.pckl'), 'rb') as f:
                self.needed_lines, self.reasons, self.vm_tasks, \
                    self.long_tasks, self.subtasks, \
                    self.stuctured_commands = pickle.load(f)
            return
        if not os.path.isdir(os.path.join(self.directory,
                                          'log_analyzer_cache')):
            os.mkdir(os.path.join(self.directory, 'log_analyzer_cache'))
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
                    subtasks, cur_needed_lines, \
                    cur_reasons = find_vm_tasks_engine(self.positions[log],
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
                    cur_reasons = find_vm_tasks_libvirtd(self.positions[log],
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
        with open(os.path.join(self.directory, 'log_analyzer_cache',
                               'vm_tasks.pckl'), 'wb') as f:
            pickle.dump([self.needed_lines, self.reasons, self.vm_tasks,
                         self.long_tasks, self.subtasks,
                         self.stuctured_commands], f)

    def find_real_line_num(self):
        self.real_line_num = {}
        if self.time_ranges == {}:
            for log in self.found_logs:
                self.real_line_num[log] = [0]
            return
        for log in self.found_logs:
            f = open_log_file(log)
            line_num = 0
            self.real_line_num[log] = []
            for pos in self.positions[log]:
                while f.tell() < pos[0]:
                    f.readline()
                    line_num += 1
                self.real_line_num[log] += [line_num]
            f.close()

    def load_data(self, show_warnings, show_progressbar):
        self.all_errors = {}
        self.format_fields = {}
        m = Manager()
        q = m.Queue()
        q_bar = Manager().Queue()
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
                                     q_bar,
                                     self.additive_link,
                                     self.user_events,
                                     self.user_hosts,
                                     self.time_ranges,
                                     self.user_vms,
                                     self.vm_timeline,
                                     self.subtasks,
                                     self.needed_lines,
                                     self.real_line_num,
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
                         q_bar,
                         self.additive_link,
                         self.user_events,
                         self.user_hosts,
                         self.time_ranges,
                         self.user_vms,
                         self.vm_timeline,
                         self.subtasks,
                         self.needed_lines,
                         self.real_line_num,
                         [mes['flow_id'] for l in self.vm_tasks.keys()
                          for t in self.vm_tasks[l].keys()
                          for mes in (self.vm_tasks)[l][t] if ('flow_id'
                          in mes.keys() and mes['flow_id'] != '')],
                         show_warnings] for i in idxs]
            widget_style = ['Load: ', progressbar.Percentage(), ' (',
                            progressbar.SimpleProgress(), ')', ' ',
                            progressbar.Bar(), ' ', progressbar.Timer(), ' ',
                            progressbar.AdaptiveETA()]
            sum_lines = []
            for log in self.found_logs:
                sum_lines += [p[1] - p[0] for p in self.positions[log]]
            sum_lines = sum(sum_lines)
            bar = ProgressBar(widgets=widget_style, max_value=sum_lines)
            pos = 0
            with Pool(processes=4) as pool:
                worker = pool.imap(star, run_args)
                while True:
                    try:
                        try:
                            while True:
                                result += [worker.next(0)]
                        except multiprocessing.TimeoutError:
                            pass
                        while not q_bar.empty():
                            pos_tmp, name = q_bar.get()
                            pos += pos_tmp
                            bar.update(pos)
                    except StopIteration:
                        break
            bar.finish()
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
                                self.all_vms, self.all_hosts, self.subtasks,
                                self.timeline, self.vm_tasks,
                                self.long_tasks, self.output_dir,
                                self.reasons, self.needed_lines,
                                self.criterias, self.vm_timeline)
        return important_events, new_fields

    def print_errors(self, errors_list, new_fields, out):
        print_only_dt_message(self.directory, errors_list, new_fields, out)


def star(input):
    return process_files(*input)


def process_files(idx, log, formats_templates, directory, time_zones,
                  positions, out_descr, q_bar, additive, user_events,
                  user_hosts, time_ranges, user_vms, vm_timeline, tasks,
                  needed_lines, real_line_num, flow_ids, show_warnings,
                  progressbar=None, text_header=None):
    if text_header:
        text_header.update_mapping(type_op="Parsing:")
    # gathering all information about errors from a logfile into lists
    lines_info, fields_names = loop_over_lines(directory,
                                               log[idx],
                                               formats_templates[log[idx]],
                                               time_zones[idx],
                                               positions[log[idx]],
                                               out_descr,
                                               q_bar,
                                               additive,
                                               user_events,
                                               user_hosts,
                                               time_ranges,
                                               user_vms,
                                               vm_timeline,
                                               tasks,
                                               needed_lines,
                                               real_line_num[log[idx]],
                                               flow_ids,
                                               show_warnings,
                                               progressbar)
    return lines_info, fields_names
