import os
import sys
import lzma
import re
from functools import partial
from multiprocessing import Pool, Manager, Queue
from lib.create_error_definition import loop_over_lines
from lib.errors_statistics import merge_all_errors_by_time, \
                                    calculate_events_frequency
from lib.represent_statistics import print_only_dt_message, print_all_headers
from lib.link_errors import create_error_graph
from lib.ProgressPool import ProgressPool

class LogAnalyzer:
    #out_descr
    #directory
    #filenames[]
    #time_zones[]
    #formats_templates{0:'...',}
    #----
    #found_logs['log1',...]
    #log_file_format{"log1":...,}
    #all_errors{"log1":...,}
    #format_fields{'log1':...}
    def __init__(self, out_descr, directory, filenames, tz, \
                time_ranges, vms, events, hosts, templates_filename, \
                vm_names, host_names):
        self.out_descr = out_descr
        self.directory = directory
        self.time_ranges = time_ranges
        self.vms = vms
        self.events = events
        self.hosts = hosts
        self.all_vms = vm_names
        self.all_hosts = host_names
        #parse formats file
        formats = open(templates_filename, 'r').read().split('\n')
        formats_templates = {}
        format_num = 0
        for line in formats:
            if line[0] == '@':
                format_name = line[1:]
            elif line[0:2] == 'r^' and format_name != '':
                try:
                    re.compile(line[1:])
                except:
                    self.out_descr.write("Wrong format of regexp: %s\n" \
                                        % line[1:])
                    exit()
                formats_templates[format_num] = line[1:]
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
            #find format of a log
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
            #save log's time zome
            self.time_zones += [tz[log]]
            for file_format_num in sorted(formats_templates.keys(), \
                                            key=lambda k:int(k)):
                prog = re.compile(formats_templates[file_format_num])
                result = prog.search(line)
                if result is not None:
                    self.log_files_format += [prog]
                    break
    def load_data(self, show_warnings):
        self.all_errors = {}
        self.format_fields = {}
        m = Manager()
        q = m.Queue()
        idxs = range(len(self.found_logs))
        result = ProgressPool(
                    [(process_files, "{}".format(self.found_logs[i]), [i, \
                                        self.found_logs, \
                                        self.log_files_format, \
                                        self.directory, \
                                        self.time_zones, \
                                        q, \
                                        self.events, \
                                        self.hosts, \
                                        self.time_ranges, \
                                        self.vms, \
                                        show_warnings]) \
                                        for i in idxs], processes = 5)
            
        for idx, log in enumerate(self.found_logs):
            self.all_errors[log] = result[idx][0]
            #saving logfile format fields names
            self.format_fields[log] = result[idx][1]
        if self.all_errors == {}:
            self.out_descr.write('No matches.\n')
            exit()
        if not q.empty():
            warns = q.get()
            for warn in warns:
                self.out_descr.write(warn)

    def find_rare_errors(self):
        timeline, merged_errors, self.all_fields = merge_all_errors_by_time(
                                            self.all_errors, self.format_fields)
        try:
            del self.all_errors
        except:
            pass
        self.timeline = timeline
        keywords = set(self.events + self.hosts + self.vms + \
                        list(self.all_vms.keys()) + \
                        [i for s in self.all_vms.values() for i in s] + \
                        list(self.all_hosts.keys()) + \
                        [i for s in self.all_hosts.values() for i in s])
        calculate_events_frequency(merged_errors, keywords, timeline, 
                                    self.all_fields)
        return merged_errors
        
    def print_errors(self, errors_list, out):
        #print_all_headers(errors_list, self.list_headers, self.format_fields, out)
        print_only_dt_message(errors_list, out, self.all_fields)

def process_files(idx, log, formats_templates, directory, time_zones, out_descr, 
                    events, hosts, time_ranges, vms, show_warnings, \
                    progressbar = None, text_header = None):
    text_header.update_mapping(type_op="Parsing:")
    # gathering all information about errors from a logfile into lists
    #print('Analysing %s' % os.path.join(directory, log[idx])+'...\n')
    lines_info, fields_names = loop_over_lines(
            directory, \
            log[idx], \
            formats_templates[idx], \
            time_zones[idx], \
            out_descr, \
            events, \
            hosts, \
            time_ranges, \
            vms, \
            show_warnings,\
            progressbar)
    return lines_info, fields_names
    
    #def dump_to_json(self, path, outfile,
    #                 template='chart_errors_statistics_template.html'):
    #    pass

    # search related errors in 5-sec sliding window
    #def create_errors_graph(self, path, outfile, step=5):
    #    sum_errors_time_first = []
    #    for log in self.found_logs:
    #        # calculating a number of errors for every moment of time (needed
    #        # for chart and graph)
    #        all_times = [str(x) for x in self.all_errors[log]['datetime']]
    #        sum_errors_time_first += [
    #            summarize_errors_time_first(self.all_errors[log]['who_send'],
    #                                        self.all_errors[log]['event'],
    #                                        self.all_errors[log]['thread'],
    #                                        self.all_errors[log]['msg_text'],
    #                                        all_times)]
    #    # summarizing errors' appearing number like in PrintStatistics
    #    if len(sum_errors_time_first) == 1 and sum_errors_time_first[0] == {}:
    #        return
    #    timeline, errors_dict = merge_all_errors_by_time(
    #        self.found_logs, sum_errors_time_first)
    #    # searching for suspicious errors, linking them with following errors
    #    create_error_graph(self.log_freq, self.sender_freq, self.event_freq,
    #                       self.message_freq, timeline, errors_dict,
    #                       os.path.join(path, outfile), step)
