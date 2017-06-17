import os
import re
from lib.create_error_definition import loop_over_lines
from lib.errors_statistics import merge_all_errors_by_time
from lib.represent_statistics import print_statistics, dump_json
from lib.link_errors import create_error_graph


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
                time_ranges, vms, events, hosts, templates_filename):
        self.out_descr = out_descr
        self.directory = directory
        self.filenames = filenames
        self.time_zones = tz
        self.time_ranges = time_ranges
        self.vms = vms
        self.events = events
        self.hosts = hosts
        #parse formats file
        formats = open(templates_filename, 'r').read().split('\n')
        self.formats_templates = {}
        format_num = 0
        for line in formats:
            if line[0] == '@':
                format_name = line[1:]
            elif line[0:2] == 'r^' and format_name != '':
                self.formats_templates[format_num] = line[1:]
                format_name = ''
                format_num += 1
            else:
                self.out_descr.write("Wrong format of template: %s\n" % line)

    def load_data(self):
        self.found_logs = []
        self.all_errors = {}
        self.log_file_format = {}
        self.format_fields = {}
        for log in self.filenames:
            self.out_descr.write('Analysing %s' % os.path.join(
                self.directory, log) + '.log ...\n')
            if not os.path.isfile(os.path.join(self.directory, log) + '.log'):
                self.out_descr.write("File not found: %s\n" % log + '.log')
                continue
            # save name of actually opened logfile
            self.found_logs += [log]
            #find format of a log
            filename = os.path.join(self.directory, log) + '.log'
            line = open(filename, 'r').readline()
            for file_format_num in sorted(self.formats_templates.keys(), \
                                            key=lambda k:int(k)):
                prog = re.compile(self.formats_templates[file_format_num])
                result = prog.search(line)
                if result is not None:
                    self.log_file_format[log] = file_format_num
                    break

            self.all_errors[log] = {}
            # gathering all information about errors from a logfile into lists
            filename = os.path.join(self.directory, log) + '.log'
            lines_info = loop_over_lines(filename, \
                    self.formats_templates[self.log_file_format[log]], \
                    self.time_zones[log], \
                    self.out_descr, \
                    self.time_ranges, \
                    self.vms, \
                    self.events, \
                    self.hosts)
            self.all_errors[log] = lines_info
            #saving logfile format fields names
            format_template = re.compile(self.formats_templates[
                                            self.log_file_format[log]])
            fields_names = list(sorted(format_template.groupindex.keys()))
            fields_names.remove("message")
            fields_names.append("line_number")
            self.format_fields[log] = fields_names

    def find_rare_errors(self):
        timeline, line_number_info = merge_all_errors_by_time(self.all_errors, self.format_fields)
        
    def print_errors(self, out):
        output_text = ''
        for log in self.found_logs:
            # creating an output string, calculating a number of appearing of
            # every sender, thread, event
            output_text += print_statistics(
                log, self.errors[log], self.errors_time[log],
                len(self.all_errors[log]['datetime']))
        out.write(output_text)

    def dump_to_json(self, path, outfile,
                     template='chart_errors_statistics_template.html'):
        return
        #sum_errors_time_first = []
        #for log in self.found_logs:
        #    # calculating a number of errors for every moment of time (needed
        #    # for chart and graph)
        #    all_errors = [str(x) for x in self.all_errors[log]['datetime']]
        #    sum_errors_time_first += [
        #        summarize_errors_time_first(self.all_errors[log]['who_send'],
        #                                    self.all_errors[log]['event'],
        #                                    self.all_errors[log]['thread'],
        #                                    self.all_errors[log]['msg_text'],
        #                                    all_errors)]
        ## Saving error's info by time (for a chart)
        #dump_json(self.found_logs, sum_errors_time_first,
        #          os.path.join(path, outfile), template)

    # search related errors in 5-sec sliding window
    def create_errors_graph(self, path, outfile, step=5):
        sum_errors_time_first = []
        for log in self.found_logs:
            # calculating a number of errors for every moment of time (needed
            # for chart and graph)
            all_times = [str(x) for x in self.all_errors[log]['datetime']]
            sum_errors_time_first += [
                summarize_errors_time_first(self.all_errors[log]['who_send'],
                                            self.all_errors[log]['event'],
                                            self.all_errors[log]['thread'],
                                            self.all_errors[log]['msg_text'],
                                            all_times)]
        # summarizing errors' appearing number like in PrintStatistics
        if len(sum_errors_time_first) == 1 and sum_errors_time_first[0] == {}:
            return
        timeline, errors_dict = merge_all_errors_by_time(
            self.found_logs, sum_errors_time_first)
        # searching for suspicious errors, linking them with following errors
        create_error_graph(self.log_freq, self.sender_freq, self.event_freq,
                           self.message_freq, timeline, errors_dict,
                           os.path.join(path, outfile), step)
