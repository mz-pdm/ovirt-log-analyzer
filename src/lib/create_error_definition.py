"""Parsing error messages from logfile
    - loop_over_lines - creates lists with errors' information from one log \
        file, including information in tracebacks' messages
    - Class LogLine - represents information about one error (datetime, \
        sender, thread, event, message)
"""
import numpy as np
import inspect
import pytz
import re
import json
from datetime import datetime

class LogLineError(Exception):
    """Base class for logline exceptions"""
    pass
class FormatTemplateError(LogLineError):
    """Raised when the line format does not match the log file template"""
    pass
class DateTimeNotFoundError(LogLineError):
    """Raised when the datetime was not parsed"""
    pass
class DateTimeFormatError(LogLineError):
    """Raised when the datetime format was not recognized"""
    pass
class MessageNotFoundError(LogLineError):
    """Raised when the message was not parsed"""
    pass
class NotSatisfyConditions(LogLineError):
    """Raised whel line's datetime is not included into user-defined time range, 
    or message doesn't contain defined VM, host or event"""
    pass

class LogLine:
    #out_descr
    #fields {"date_time": [], ..., "filtered_message": [], "line_number": []}
    def __init__(self, fields_names, out_descr, \
                vm_numbers, time_ranges, events, host_ids):
        self.out_descr = out_descr
        self.time_ranges = time_ranges
        self.vm_numbers = vm_numbers
        self.events = events
        self.host_ids = host_ids
        self.fields = {}
        for field in fields_names:
            self.fields[field] = ''

    def parse_date_time(self, time_zone, line):
        #datetime formats:
        #2017-05-12T07:36:00.065548Z
        #2017-05-12 07:35:59.929+0000
        #2017-05-12 03:26:25,540-0400
        #2017-05-12 03:23:31,135-04
        #2017-05-12 03:26:22,349
        #2017-05-12 03:28:13
        dt = re.findall(
            r"[0-9\-]{10}[\sT][0-9]{2}:[0-9]{2}:[0-9]{2}[\.\,0-9]*[\+\-0-9Z]*",\
            line)
        if len(dt) == 0:
            raise DateTimeNotFoundError('Warning: parse_date_time: '+\
                            'Line does not have date_time field: %s\n' % line)
        dt = dt[0]
        dt = dt.replace('T', ' ')
        dt = dt.replace('Z', '+0000')
        dt = dt.replace('.', ',')
        if ('+' in dt.partition(' ')[2] and len(dt.partition('+')[2]) < 4) \
            or ('-' in dt.partition(' ')[2] and \
                len((dt.partition(' ')[2]).partition('-')[2]) < 4):
            dt += '00'
        dt_formats = ["%Y-%m-%d %H:%M:%S,%f%z", \
                        "%Y-%m-%d %H:%M:%S%z"]
        for dt_format in dt_formats:
            try:
                date_time = datetime.strptime(dt, dt_format)
                date_time = date_time.astimezone(pytz.utc)
                self.fields['date_time'] = date_time.timestamp()
                return
                #self.out_descr.write('Time: %s\n' % date_time)
            except ValueError:
                continue
        #if we have time without time zone
        dt += time_zone
        for dt_format in dt_formats:
            try:
                date_time = datetime.strptime(dt, dt_format)
                date_time = date_time.astimezone(pytz.utc)
                self.fields['date_time'] = date_time.timestamp()
                return
                #self.out_descr.write('Time: %s\n' % date_time)
            except ValueError:
                continue
        raise DateTimeFormatError("Warning: Unknown date_time format: %s\n" % \
                                    dt)

    def parse_fields(self, pattern, line):
        fields = pattern.search(line)
        if fields is None:
            raise FormatTemplateError()
        fields = fields.groupdict()
        if self.fields["date_time"] == '':
            pass #STH WRONG
        for field in sorted(fields.keys()):
            if field == "date_time":
                continue
            self.fields[field] = fields[field]

    def parse_message(self, custom_message_text = None):
        if custom_message_text is not None:
            mstext = custom_message_text
        else:
            mstext = self.fields['message']
        if mstext == '':
            raise MessageNotFoundError()
        template = re.compile(r'([Mm]essage[\:\=\ \'\"]+)(.+)')
        t = re.search(template, mstext)
        if t is not None:
            mstext = t.group(2)
        #template = re.compile(\
        #    r"[^^]\"[^\"]{20,}\"|"+\
        #    r"[^^]\'[^\']{20,}\'|"+\
        #    r"[^^]\[+.*\]+|"+\
        #    r"[^^]\(+.*\)+|"+\
        #    r"[^^]\{+.*\}+|"+\
        #    r"[^^]\<+.*\>+|"+\
        #    r"[^^][^\ \t\,\;\=]{20,}|"+\
        #    r"[\d]+")
        #mstext = re.sub(template, '<...>', mstext)
        #mstext = re.sub(re.compile(
        #    r"((\<\.\.\.\>[\ \.\,\:\;\{\}\(\)\[\]\$]*){2,})"), '<...>', mstext)
        #mstext = re.sub(re.compile(
        #    r"(([\ \.\,\:\;\+\-\{\}]*"+\
        #    r"\<\.\.\.\>[\ \.\,\:\;\+\-\{\}]*)+)"), '<...>', mstext)
        self.fields["filtered_message"] = re.sub(
            r'^[\ \t\.\,\:\=]+|[\ \t\.\,\n]+$', '', \
            mstext)

    def check_constraints(self):
        for needed_dt in range(0, len(self.time_ranges)-1, 2):
            if self.fields["date_time"] < self.time_ranges[needed_dt] or \
                    self.fields["date_time"] > self.time_ranges[needed_dt+1]:
                raise NotSatisfyConditions()
        for needed_vm in self.vm_numbers:
            if not needed_vm in self.fields["filtered_message"]:
                raise NotSatisfyConditions()
        for needed_host in self.host_ids:
            if not needed_host in self.fields["filtered_message"]:
                raise NotSatisfyConditions()
        for needed_event in self.events:
            if not needed_event in self.fields["filtered_message"]:
                raise NotSatisfyConditions()
#def parallelize_parsing(logname, format_template, time_zome, out_descr):
#    file_lines = {}
#    format_template = re.compile(format_template)
#    #fields_names = list(sorted(format_template.groupindex.keys()))
#    #fields_names.remove("message")
#    fields_names = ["date_time"]
#    n_threads = 5
#    f = open(logname, 'r')
#    n = f.seek(0, 2)
#    print(">>", n)
#    for thread_pos in range(0, n-n%n_threads, n//n_threads):
#        print(thread_pos)
#        f.seek(thread_pos)
#        f.readline()
#        line = f.readline()
#        if LogLine(fields_names, out_descr).parse_date_time(time_zome, line)

def loop_over_lines(logname, format_template, time_zome, out_descr, \
                    time_ranges, vm_numbers, events, host_ids):
    file_lines = {}
    format_template = re.compile(format_template)
    #fields_names = list(sorted(format_template.groupindex.keys()))
    #fields_names.remove("message")
    fields_names = ["date_time"]
    with open(logname) as f:
        prev_fields = {}
        in_traceback_line = ''
        in_traceback_flag = False
        multiline_line = ''
        multiline_flag = False
        for line_num, line in enumerate(f):
            if len(re.findall(r"^(\ *)$", line)) != 0:
                continue
            line_data = LogLine(fields_names, out_descr, \
                        vm_numbers, time_ranges, events, host_ids)
            try:
                line_data.parse_date_time(time_zome, line)
                line_data.parse_fields(format_template, line)
                line_data.parse_message()
                line_data.check_constraints()
                if in_traceback_flag:
                    try:
                        mess = LogLine(fields_names, out_descr, \
                                vm_numbers, time_ranges, events, host_ids)
                        mess.parse_message(prev_fields["filtered_message"]+\
                                            ' '+in_traceback_line)
                        mess.check_constraints()
                        if mess.fields["filtered_message"] not in file_lines.keys():
                            file_lines[mess.fields["filtered_message"]] = []                
                        line_info = []
                        for field in fields_names:
                            line_info += [prev_fields[field]]
                        line_info += [prev_line_number]
                        file_lines[mess.fields["filtered_message"]]+=line_info
                        in_traceback_flag = False
                    except NotSatisfyConditions:
                        prev_fields = {}
                        continue
                elif multiline_flag:
                    try:
                        mess = LogLine(fields_names, out_descr,\
                                vm_numbers, time_ranges, events, host_ids)
                        mess.parse_date_time(time_zome, multiline_line)
                        mess.parse_fields(format_template, multiline_line)
                        mess.parse_message()
                        mess.check_constraints()
                        if mess.fields["filtered_message"] not in \
                                                            file_lines.keys():
                            file_lines[mess.fields["filtered_message"]] = []                
                        line_info = []
                        for field in fields_names:
                            line_info += [mess.fields[field]]
                        line_info += [prev_line_number]
                        file_lines[mess.fields["filtered_message"]]+=line_info
                    except NotSatisfyConditions:
                        prev_fields = {}
                        continue
                    except (DateTimeNotFoundError, DateTimeFormatError) as \
                                                            exception_message:
                        if multiline_line not in file_lines.keys():
                            file_lines[multiline_line] = []                
                        line_info = []
                        for field in fields_names:
                            line_info += [prev_fields[field]]
                        line_info += [prev_line_number]
                        file_lines[multiline_line] += line_info
                    except FormatTemplateError as exception_message:
                        #out_descr.write('Warning: parse_fields: '+\
                        #    'Line does not match format "%s": %s\n'% \
                        #    (format_template, line))
                        if '!Fake datetime! ' + multiline_line not in \
                                                            file_lines.keys():
                            file_lines['!Fake datetime! ' + multiline_line] = []                
                        line_info = []
                        for field in fields_names:
                            line_info += [prev_fields[field]]
                        line_info += [prev_line_number]
                        file_lines['!Fake datetime! '+multiline_line]+=line_info
                    multiline_flag = False
                elif prev_fields != {}:
                    if prev_fields["filtered_message"] not in file_lines.keys():
                        file_lines[prev_fields["filtered_message"]] = []
                    line_info = []
                    for field in fields_names:
                        line_info += [prev_fields[field]]
                    line_info += [prev_line_number]
                    file_lines[prev_fields["filtered_message"]] += line_info
                else:
                    pass
                prev_fields = line_data.fields
                prev_line_number = line_num
            except NotSatisfyConditions:
                if prev_fields != {}:
                    if prev_fields["filtered_message"] not in file_lines.keys():
                        file_lines[prev_fields["filtered_message"]] = []
                    line_info = []
                    for field in fields_names:
                        line_info += [prev_fields[field]]
                    line_info += [prev_line_number]
                    file_lines[prev_fields["filtered_message"]] += line_info
                    prev_fields = {}
                continue
            except (DateTimeNotFoundError, DateTimeFormatError) as \
                                            exception_message:
                if prev_fields == {}:
                    out_descr.write(str(exception_message))
                    continue
                if prev_fields["filtered_message"] not in file_lines.keys():
                    file_lines[prev_fields["filtered_message"]] = []
                line_info = []
                for field in fields_names:
                    line_info += [prev_fields[field]]
                line_info += [prev_line_number]
                file_lines[prev_fields["filtered_message"]] += line_info
                if 'Traceback' in line:
                    #Just remember that we are in traceback
                    in_traceback_flag = True
                    in_traceback_line = ''
                elif in_traceback_flag and not multiline_flag:
                    #Remember a line if we are in a traceback. 
                    #The last line will be concatenated with a message
                    line = re.sub(r'^[\t\.\,\=]+|[\t\.\,\n]+$', '', line)
                    in_traceback_line += line
                elif multiline_flag:
                    line = re.sub(r'^[\t\.\,\=]+|[\t\.\,\n]+$', '', line)
                    multiline_line += line
                else:
                    #The analyzer didn't find a datetime in a line, 
                    #the message will receive datetime from previous 
                    #message with a mark "Fake datetime"
                    #out_descr.write(str(exception_message))
                    multiline_flag = True
                    line = re.sub(r'^[\t\.\,\=]+|[\t\.\,\n]+$', '', line)
                    multiline_line = '!Fake datetime! ' + line
                    prev_line_number = line_num
            except FormatTemplateError as exception_message:
                #out_descr.write('Warning: parse_fields: '+\
                #            'Line does not match format "%s": %s\n'% \
                #            (format_template, line))
                #We are in a line with datetime, but the analyzer didn't 
                #find all fields from a template
                if prev_fields["filtered_message"] not in file_lines.keys():
                    file_lines[prev_fields["filtered_message"]] = []
                line_info = []
                for field in fields_names:
                    line_info += [prev_fields[field]]
                line_info += [prev_line_number]
                file_lines[prev_fields["filtered_message"]] += line_info

                multiline_flag = True
                line = re.sub(r'^[\t\.\,\=]+|[\t\.\,\n]+$', '', line)
                multiline_line = line
                prev_line_number = line_num

            except MessageNotFoundError as exception_message:
                out_descr.write('Warning: parse_message: '+\
                            'Line does not have message '+'field: %s\n' % line)
        #adding the last line
        if in_traceback_flag:
            try:
                mess = LogLine(fields_names, out_descr,\
                        vm_numbers, time_ranges, events, host_ids)
                mess.parse_message(prev_fields["filtered_message"]+\
                                    ' '+in_traceback_line)
                mess.check_constraints()
                if mess.fields["filtered_message"] not in file_lines.keys():
                    file_lines[mess.fields["filtered_message"]] = []                
                line_info = []
                for field in fields_names:
                    line_info += [prev_fields[field]]
                line_info += [prev_line_number]
                file_lines[mess.fields["filtered_message"]]+=line_info
                in_traceback_flag = False
            except NotSatisfyConditions:
                pass
        elif multiline_flag:
            try:
                mess = LogLine(fields_names, out_descr,\
                        vm_numbers, time_ranges, events, host_ids)
                mess.parse_date_time(time_zome, multiline_line)
                mess.parse_fields(format_template, multiline_line)
                mess.parse_message()
                mess.check_constraints()
                if mess.fields["filtered_message"] not in file_lines.keys():
                    file_lines[mess.fields["filtered_message"]] = []                
                line_info = []
                for field in fields_names:
                    line_info += [mess.fields[field]]
                line_info += [prev_line_number]
                file_lines[mess.fields["filtered_message"]]+=line_info
            except NotSatisfyConditions:
                pass
            except FormatTemplateError as exception_message:
                #out_descr.write('Warning: parse_fields: '+\
                #    'Line does not match format "%s": %s\n'% \
                #    (format_template, line))
                if '!Fake datetime! ' + line not in file_lines.keys():
                    file_lines['!Fake datetime! ' + line] = []                
                line_info = []
                for field in fields_names:
                    line_info += [prev_fields[field]]
                line_info += [prev_line_number]
                file_lines['!Fake datetime! ' + line]+=line_info
            multiline_flag = False
        elif prev_fields != {}:
            if prev_fields["filtered_message"] not in file_lines.keys():
                file_lines[prev_fields["filtered_message"]] = []
            line_info = []
            for field in fields_names:
                line_info += [prev_fields[field]]
            line_info += [prev_line_number]
            file_lines[prev_fields["filtered_message"]] += line_info
        else:
            pass

    f = open("lines_"+logname.split('/')[-1][:-4]+".js", 'w')
    json.dump(file_lines, f, indent=4, sort_keys=True)
    f.close()
    return file_lines

