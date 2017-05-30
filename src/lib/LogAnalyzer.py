import os

from lib.create_error_definition import loop_over_lines
from lib.errors_statistics import \
    summarize_errors, summarize_errors_time_first, \
    merge_all_errors_by_time, calculate_frequency
from lib.represent_statistics import print_statistics, dump_json
from lib.link_errors import create_error_graph


class LogAnalyzer:

    def __init__(self, out_descr, directory, filenames):
        self.out_descr = out_descr
        self.directory = directory
        self.filenames = filenames

    def load_data(self):
        self.found_logs = []
        self.all_errors = {}
        for log in self.filenames:
            self.out_descr.write('Analysing %s' % os.path.join(
                self.directory, log) + '.log ...\n')
            if not os.path.isfile(os.path.join(self.directory, log) + '.log'):
                self.out_descr.write("File not found: %s\n" % log + '.log')
                continue
            # save name of actually opened logfile
            self.found_logs += [log]
            self.all_errors[log] = {}
            # gathering all information about errors from a logfile into lists
            filename = os.path.join(self.directory, log) + '.log'
            error_datetime, error_who_send, error_thread, error_event, \
                error_msg_text = loop_over_lines(filename, self.out_descr)
            self.all_errors[log]['datetime'] = error_datetime
            self.all_errors[log]['who_send'] = error_who_send
            self.all_errors[log]['thread'] = error_thread
            self.all_errors[log]['event'] = error_event
            self.all_errors[log]['msg_text'] = error_msg_text

    def merge_logfiles(self):
        self.errors = {}
        self.errors_time = {}
        for log in self.found_logs:
            # calculating a number of every error's appearing in a file and its
            # time
            sum_errors, sum_errors_time = summarize_errors(
                self.all_errors[log]['who_send'],
                self.all_errors[log]['event'],
                self.all_errors[log]['thread'],
                self.all_errors[log]['msg_text'],
                self.all_errors[log]['datetime'])
            self.errors[log] = sum_errors
            self.errors_time[log] = sum_errors_time

    def calculate_errors_frequency(self):
        # summarizing errors' appearing number like in PrintStatistics
        self.log_freq, self.sender_freq, self.event_freq, self.message_freq = \
                            calculate_frequency(self.found_logs, self.errors)

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
        sum_errors_time_first = []
        for log in self.found_logs:
            # calculating a number of errors for every moment of time (needed
            # for chart and graph)
            all_errors = [str(x) for x in self.all_errors[log]['datetime']]
            sum_errors_time_first += [
                summarize_errors_time_first(self.all_errors[log]['who_send'],
                                            self.all_errors[log]['event'],
                                            self.all_errors[log]['thread'],
                                            self.all_errors[log]['msg_text'],
                                            all_errors)]
        # Saving error's info by time (for a chart)
        dump_json(self.found_logs, sum_errors_time_first,
                  os.path.join(path, outfile), template)

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
