"""Parsing error messages from logfile
    - loop_over_lines - creates lists with errors' information from one log \
        file, including information in tracebacks' messages
    - Class LogLine - represents information about one error (datetime, \
        sender, thread, event, message)
"""
import numpy as np
import pytz
import re
import os
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
class DateTimeNotInTimeRange(LogLineError):
    """Raised when datetime is not included into user-defined time range"""
    pass
class SatisfyConditions(LogLineError):
    """Raised when message doesn't contain defined VM, host or event"""
    pass

class LogLine:
    def __init__(self, fields_names, line_num, out_descr, time_ranges):
        self.out_descr = out_descr
        self.time_ranges = time_ranges
        self.raw_line = ''
        self.msg_template = re.compile(r'(Message[\:\=\ ]+)(.+)')
        self.fields = {}
        for field in fields_names:
            self.fields[field] = ''
        self.fields['line_num'] = line_num

    def parse_date_time(self, time_zone, line):
        #datetime formats:
        #2017-05-12T07:36:00.065548Z
        #2017-05-12 07:35:59.929+0000
        #2017-05-12 03:26:25,540-0400
        #2017-05-12 03:23:31,135-04
        #2017-05-12 03:26:22,349
        #2017-05-12 03:28:13
        self.raw_line = line
        dt = re.findall(
            r"[0-9\-]{10}[\sT][0-9]{2}:[0-9]{2}:[0-9]{2}[\.\,0-9]*[\+\-0-9Z]*",\
            line)
        if len(dt) == 0 or line[0] in [' ', '\t']:
            raise DateTimeNotFoundError('Warning: parse_date_time: '+\
                            'Line does not have date_time field: %s\n' % line)
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
                self.fields['date_time'] = date_time.timestamp()
                break
                #self.out_descr.write('Time: %s\n' % date_time)
            except ValueError:
                continue
        if self.fields['date_time'] == '':
            raise DateTimeFormatError("Warning: Unknown date_time format: "+\
                                        "%s\n" % dt)
        #Check user-defined time range
        if self.time_ranges != [] and \
            not any([self.fields['date_time'] >= tr[0] and \
                    self.fields['date_time'] <= tr[1] \
                    for tr in self.time_ranges]):
            raise DateTimeNotInTimeRange()

    def parse_fields(self, pattern, line):
        line = re.sub(r'^[\t\ ]+|[\t\ ]+$', '', line)
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
        t = re.search(self.msg_template, mstext)
        if t is not None:
            mstext = t.group(2)
        self.fields["message"] = re.sub(
            r'^[\ \t\.\,\:\=]+|[\ \t\.\,\n]+$', '', \
            mstext)

def check_constraints(line, events, host_ids, vm_numbers):
    if any([keyword in line for keyword in events + \
                                                host_ids + \
                                                vm_numbers + \
                                                ["ERROR", "Traceback"]]):
        return True
    else:
        return False

def create_line_info(in_traceback_flag, in_traceback_line, multiline_flag, \
                        multiline_line, fields_names, out_descr, time_zome, \
                        time_ranges, events, host_ids, vm_numbers, \
                        format_template, prev_fields, prev_line,\
                        show_warnings):
    if in_traceback_flag:
        in_traceback_flag = False
        prev_line = prev_line + in_traceback_line
        if not check_constraints(prev_line, events, host_ids, vm_numbers):
            return prev_line, [], in_traceback_flag, multiline_flag
        try:
            mess = LogLine(fields_names, prev_fields['line_num'], \
                            out_descr, time_ranges)
            mess.parse_message(prev_fields["message"]+\
                                ' '+in_traceback_line)
            line_info = []
            for field in fields_names:
                line_info += [prev_fields[field]]
            line_info += [mess.fields["message"]]
            return prev_line, line_info, in_traceback_flag, multiline_flag
        except MessageNotFoundError as exception_message:
            if show_warnings:
                out_descr.write('Warning: parse_message: '+\
                            'Line does not have message field: %s\n' % \
                            prev_fields["message"]+\
                                ' '+in_traceback_line)
            return prev_line, [], in_traceback_flag, multiline_flag
    elif multiline_flag:
        multiline_flag = False
        prev_line = multiline_line
        if not check_constraints(prev_line, events, host_ids, vm_numbers):
            return prev_line, [], in_traceback_flag, multiline_flag
        try:
            mess = LogLine(fields_names, prev_fields['line_num'], \
                            out_descr, time_ranges)
            mess.parse_date_time(time_zome, multiline_line)
            mess.parse_fields(format_template, multiline_line)
            mess.parse_message()               
            line_info = []
            for field in fields_names:
                line_info += [mess.fields[field]]
            line_info += [mess.fields["message"]]
            return prev_line, line_info, in_traceback_flag, multiline_flag
        except (DateTimeNotFoundError, DateTimeFormatError) as \
                                                exception_message:
            line_info = []
            for field in fields_names:
                line_info += [prev_fields[field]]
            line_info += [multiline_line]
            return prev_line, line_info, in_traceback_flag, multiline_flag
        except FormatTemplateError as exception_message:
            line_info = []
            for field in fields_names:
                line_info += [prev_fields[field]]
            line_info += ['!Fake datetime! ' + multiline_line]
            return prev_line, line_info, in_traceback_flag, multiline_flag
        except MessageNotFoundError as exception_message:
            if show_warnings:
                out_descr.write('Warning: parse_message: '+\
                        'Line does not have message field: %s\n' % line)
            return prev_line, [], in_traceback_flag, multiline_flag
    else:
        if not check_constraints(prev_line, events, host_ids, vm_numbers):
            return prev_line, [], in_traceback_flag, multiline_flag
        line_info = []
        for field in fields_names:
            line_info += [prev_fields[field]]
        line_info += [prev_fields["message"]]
        return prev_line, line_info, in_traceback_flag, multiline_flag

def loop_over_lines(directory, logname, format_template, time_zome, out_descr, \
                    events, host_ids, time_ranges, vm_numbers, show_warnings):
    full_filename = os.path.join(directory, logname) + '.log'
    format_template = re.compile(format_template)
    fields_names = list(sorted(format_template.groupindex.keys()))
    fields_names.remove("message")
    fields_names.remove("date_time")
    fields_names = ['date_time', 'line_num'] + fields_names
    #out = open('result_'+logname+'.txt', 'w')
    file_lines = []
    with open(full_filename) as f:
        prev_fields = {}
        in_traceback_line = ''
        in_traceback_flag = False
        multiline_line = ''
        multiline_flag = False
        #count = 0
        store = True
        for line_num, line in enumerate(f):
            if len(re.findall(r"^(\ *)$", line)) != 0 or ('libvirt' in logname\
                                                     and "OBJECT_" in line):
                #the line is empty
                continue
            line_data = LogLine(fields_names, logname+':'+str(line_num), \
                                out_descr, time_ranges)
            try:
                line_data.parse_date_time(time_zome, line)
                line_data.parse_fields(format_template, line)
                line_data.parse_message()
                if store and prev_fields != {}:
                    prev_line, line_info, \
                    in_traceback_flag, \
                    multiline_flag = create_line_info(in_traceback_flag, \
                            in_traceback_line, multiline_flag, \
                            multiline_line, fields_names, out_descr, \
                            time_zome, time_ranges, events, host_ids, \
                            vm_numbers, format_template, \
                            prev_fields, prev_line, show_warnings)
                    if line_info != []:
                        file_lines += [line_info]
                        #count += 1
                        #out.write('>>>%d' % count)
                        #out.write(prev_line)
                        #out.write('\n')
                prev_fields = line_data.fields
                prev_line = line
                store = True
            except DateTimeNotInTimeRange:
                if store and prev_fields != {}:
                    prev_line, line_info, \
                    in_traceback_flag, \
                    multiline_flag = create_line_info(in_traceback_flag, \
                            in_traceback_line, multiline_flag, \
                            multiline_line, fields_names, out_descr, \
                            time_zome, time_ranges, events, host_ids, \
                            vm_numbers, format_template, \
                            prev_fields, prev_line, show_warnings)
                    if line_info != []:
                        file_lines += [line_info]
                        #count += 1
                        #out.write('>>>%d'%count)
                        #out.write(prev_line)
                        #out.write('\n')
                store = False
            except (DateTimeNotFoundError, DateTimeFormatError) as \
                                            exception_message:
                if prev_fields == {}:
                    if show_warnings:
                        out_descr.write(str(line_num) + ': ')
                        out_descr.write(str(exception_message))
                    continue
                if in_traceback_flag:
                    #Remember a line if we are in a traceback. 
                    #The line will be concatenated with a message
                    line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\n]+$', ' ', line)
                    in_traceback_line += line
                elif multiline_flag:
                    line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\n]+$', ' ', line)
                    multiline_line += line
                elif 'Traceback' in line or re.match(
                            r'^[\t\ ]*[at,Caused,\.\.\.].+', line) is not None:
                    #We are in a traceback
                    in_traceback_flag = True
                    line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\n]+$', ' ', line)
                    in_traceback_line = line
                else:
                    #The analyzer didn't find a datetime in a line, 
                    #the message will receive datetime from previous 
                    #message with a mark "Fake datetime"
                    if show_warnings:
                        out_descr.write(str(exception_message))
                    if store and prev_fields != {}:
                        prev_line, line_info, \
                        in_traceback_flag, \
                        multiline_flag = create_line_info(in_traceback_flag, \
                                in_traceback_line, multiline_flag, \
                                multiline_line, fields_names, out_descr, \
                                time_zome, time_ranges, events, host_ids, \
                                vm_numbers, format_template, \
                                prev_fields, prev_line, show_warnings)
                        if line_info != []:
                            file_lines += [line_info]
                            #count += 1
                            #out.write('>>>%d'%count)
                            #out.write(prev_line)
                            #out.write('\n')
                    multiline_flag = True
                    line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\n]+$', ' ', line)
                    multiline_line = '!Fake datetime! ' + line
            except FormatTemplateError as exception_message:
                if show_warnings:
                    out_descr.write('Warning: parse_fields: '+\
                                'Line does not match format "%s": %s\n'% \
                                (format_template, line))
                #We are in a line with datetime, but the analyzer didn't 
                #find all fields from a template
                if store and prev_fields != {}:
                    prev_line, line_info, \
                    in_traceback_flag, \
                    multiline_flag = create_line_info(in_traceback_flag, \
                            in_traceback_line, multiline_flag, \
                            multiline_line, fields_names, out_descr, time_zome,\
                            time_ranges, events, host_ids, vm_numbers, \
                            format_template, prev_fields, \
                            prev_line, show_warnings)
                    if line_info != []:
                        file_lines += [line_info]
                        #count += 1
                        #out.write('>>>%d'%count)
                        #out.write(prev_line)
                        #out.write('\n')
                multiline_flag = True
                line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\n]+$', '', line)
                multiline_line = line
            except MessageNotFoundError as exception_message:
                if show_warnings:
                    out_descr.write('Warning: parse_message: '+\
                            'Line does not have message field: %s\n' % line)
        #adding the last line
        if store and prev_fields != {}:
            prev_line, line_info, \
            in_traceback_flag, \
            multiline_flag = create_line_info(in_traceback_flag, \
                    in_traceback_line, multiline_flag, \
                    multiline_line, fields_names, out_descr, time_zome, \
                    time_ranges, events, host_ids, vm_numbers, \
                    format_template, prev_fields, \
                    prev_line, show_warnings)
            if line_info != []:
                file_lines += [line_info]
                #count += 1
                #out.write('>>>%d'%count)
                #out.write(prev_line)
                #out.write('\n')
    return file_lines, fields_names + ['message']
