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

class LogLine:
    def __init__(self, line_number, format_t, out_descr):
        self.out_descr = out_descr
        self.line_number = line_number
        self.fields = {}
        fields_names = format_t['template'].split(' ')
        for field in fields_names:
            self.fields[field] = ''

    def parse_fields(self, line, format_t):
        fields_names = format_t['template'].split(' ')
        fields = re.search(format_t['regexp'], line)
        fields.groupdict()
        print(fields)
        for field in sorted(fields.keys()):
            if field == 'date_time' and fields[field] == '':
                raise FormatTemplateError()
        self.fields = fields

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
            raise DateTimeNotFoundError()
        dt = dt[0]
        dt = dt.replace('T', ' ')
        dt = dt.replace('Z', '+0000')
        dt = dt.replace('.', ',')
        if ('+' in dt.partition(' ')[2] and len(dt.partition('+')[2]) < 4) \
            or ('-' in dt.partition(' ')[2] and \
                len((dt.partition(' ')[2]).partition('-')[2]) < 4):
            dt += '00'
        print('>>> dt = ', dt)
        dt_formats = ["%Y-%m-%d %H:%M:%S,%f%z", \
                        "%Y-%m-%d %H:%M:%S%z"]
        for dt_format in dt_formats:
            try:
                date_time = datetime.strptime(dt, dt_format)
                self.fields['date_time'] = str(date_time.astimezone(pytz.utc))
                return
                #self.out_descr.write('Time: %s\n' % date_time)
            except ValueError:
                continue
        #if we have time without time zone
        dt += time_zone
        for dt_format in dt_formats:
            try:
                date_time = datetime.strptime(dt, dt_format)
                self.fields['date_time'] = str(date_time.astimezone(pytz.utc))
                return
                #self.out_descr.write('Time: %s\n' % date_time)
            except ValueError:
                continue
        raise DateTimeFormatError(dt)

    def parse_message(self, custom_message_text = None):
        if custom_message_text is not None:
            mstext = custom_message_text
        else:
            mstext = self.fields['message']
        if mstext == '':
            raise MessageNotFoundError()
        #template = re.compile(r'([Ee]rr[or]*|[Mm]essage)[\:\=\ \'\"\>]+(.+)')
        template = re.compile(r'([Mm]essage[\:\=\ \'\"]+)(.+)')
        t = re.search(template, mstext)
        if t is not None:
            mstext = t.group(2)
        template = re.compile(\
            r"[^^][^\ \t\n\,]+=[^\ \t\n\,]+[\W]+|[^^]\"[^\"]{20,}\"|"+\
            r"[^^]\'[^\']{20,}\'|[^^]\[+.{20,}\]+|"+\
            r"[^^]\(+.{20,}\)+|[^^]\{+.{20,}\}+|[^^]\<+.{20,}\>+|"+\
            r"[^^][^\ \t\,\.\;\:]{20,}|[^^][\d\.\:]{10,}|"+\
            r"\[+[\d]*\]+|\(+[\d]*\)+|\{+[\d]*\}+|\<+[\d]*\>+")
        mstext = re.sub(template, '<...>', mstext)
        mstext = re.sub(re.compile(r"((\<\.\.\.\>){2,})"), '<...>', mstext)
        self.filtered_message = re.sub(r'^[\ \t\.\,\:\=]+|[\ \t\.\,\n]+$', '', \
                                        mstext)

def loop_over_lines(logname, format_template, time_zome, out_descr):
    file_lines = {}
    f = re.compile(format_template)
    fields_names = list(f.groupindex.keys())
    fields_names.remove("message")
    return
    with open(logname) as f:
        prev_fields = {}
        prev_message = ''
        in_traceback_line = ''
        in_traceback_flag = False
        multiline_line = ''
        multiline_flag = False
        for line_num, line in enumerate(f):
            if len(re.findall(r"^(\ *)$", line)) != 0:
                continue
            line_data = LogLine(line_num, format_template, out_descr)
            try:
                line_data.parse_date_time(time_zome, line)
                line_data.parse_fields(line, format_template)
                line_data.parse_message()
                fields = line_data.fields
                filtered_message = line_data.filtered_message
                if in_traceback_flag:
                    if prev_message+' '+in_traceback_line not in \
                                        file_lines.keys():
                        file_lines[prev_message+' '+in_traceback_line] = []                
                    line_info = []
                    for field in fields_names:
                        line_info += [prev_fields[field]]
                    line_info += [prev_line_number]
                    file_lines[prev_message+' '+in_traceback_line]+=[line_info]
                    in_traceback_flag = False
                elif multiline_flag:
                    prev_fields = fields
                    prev_message = filtered_message
                    prev_line_number = line_num
                    mess = LogLine(line_num, format_template, out_descr)
                    mess.parse_fields(multiline_line, format_template)
                    mess.parse_message()
                    if mess.filtered_message not in \
                                        file_lines.keys():
                        file_lines[mess.filtered_message] = []
                    line_info = []
                    for field in fields_names:
                        line_info += [mess.fields[field]]
                    line_info += [prev_line_number]
                    file_lines[mess.filtered_message] += [line_info]
                    multiline_flag = False
                elif prev_message != '':
                    if prev_message not in file_lines.keys():
                        file_lines[prev_message] = []
                    line_info = []
                    for field in fields_names:
                        line_info += [prev_fields[field]]
                    line_info += [prev_line_number]
                    file_lines[prev_message] += [line_info]
                prev_fields = fields
                prev_message = filtered_message
                prev_line_number = line_num
            except FormatTemplateError as exception_message:
                if 'Traceback' in line:
                    #Just remebber that we are in traceback
                    in_traceback_flag = True
                elif in_traceback_flag:
                    #Remember a line if we are in a traceback. 
                    #The last line will be concatenated with a message
                    mess = LogLine(line_num, format_template, out_descr)
                    mess.parse_message(line)
                    in_traceback_line = mess.filtered_message
                elif multiline_flag:
                    print("mintiline flag")
                    if line_data.fields["date_time"] == '':
                        multiline_line += line
                    else:
                        mess = LogLine(line_num, format_template, out_descr)
                        mess.parse_message(multiline_line)
                        if mess.filtered_message not in \
                                            file_lines.keys():
                            file_lines[mess.filtered_message] = []
                        line_info = []
                        for field in fields_names:
                            line_info += [mess.fields[field]]
                        line_info += [prev_line_number]
                        file_lines[mess.filtered_message] += [line_info]
                    multiline_flag = False
                elif line_data.fields["date_time"] != '':
                    #We are in a line with datetime, but the analyzer didn't 
                    #find all fields from a template
                    print("mintiline flag")
                    multiline_flag = True
                    multiline_line = line
                    prev_line_number = line_num
                else:
                    #The analyzer didn't find a datetime in a line, 
                    #the message will receive datetime from previous 
                    #message with a mark "Fake datetime"
                    out_descr.write('Warning: parse_fields: '+\
                            'Line does not match format "%s": %s\n'% \
                            (format_template['name'], line))
                    if multiline_flag:
                        multiline_line += line
                    elif '!Fake datetime!' in prev_message:
                        mess = LogLine(line_num, format_template, out_descr)
                        mess.parse_message(line)
                        prev_message += mess.filtered_message
                        prev_message = re.sub(re.compile(r"((\<\.\.\.\>){2,})"), 
                                                '<...>', prev_message)
                    else:
                        mess = LogLine(line_num, format_template, out_descr)
                        mess.parse_message(line)
                        prev_message = '!Fake datetime! ' + \
                                    mess.filtered_message
                        prev_line_number = line_num
            except DateTimeNotFoundError as exception_message:
                out_descr.write('Warning: parse_date_time: '+\
                            'Line does not have date_time field: %s\n' % line)
            except DateTimeFormatError as exception_message:
                out_descr.write('Warning: Unknown date_time format: %s\n' % \
                                    str(exception_message))
            except MessageNotFoundError as exception_message:
                out_descr.write('Warning: parse_message: '+\
                            'Line does not have message '+'field: %s\n' % line)
        #adding the last line
        print("last_line...")
        if in_traceback_flag:
            if prev_message+' '+in_traceback_line not in \
                                file_lines.keys():
                file_lines[prev_message+' '+in_traceback_line] = []
            line_info = []
            for field in fields_names:
                line_info += [prev_fields[field]]
            line_info += [prev_line_number]
            file_lines[prev_message+' '+in_traceback_line] = [line_info]
            in_traceback_flag = False
        elif multiline_flag:
            mess = LogLine(prev_line_number, format_template, out_descr)
            mess.parse_fields(multiline_line, format_template)
            mess.parse_message()
            if mess.filtered_message not in \
                                file_lines.keys():
                file_lines[mess.filtered_message] = []
            line_info = []
            for field in fields_names:
                line_info += [mess.fields[field]]
            line_info += [prev_line_number]
            file_lines[mess.filtered_message] += [line_info]
            multiline_flag = False
        else:
            if prev_message not in file_lines.keys():
                file_lines[prev_message] = []
            line_info = []
            for field in fields_names:
                line_info += [prev_fields[field]]
            line_info += [prev_line_number]
            file_lines[prev_message] += [line_info]

    f = open("lines_"+logname.split('/')[-1][:-4]+".js", 'w')
    json.dump(file_lines, f, indent=4, sort_keys=True)
    f.close()
    return file_lines
