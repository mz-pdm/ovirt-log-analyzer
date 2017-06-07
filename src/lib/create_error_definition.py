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
    #fields {'date_time':, 'message':, ...}
    #out_descr
    def __init__(self, line_number, out_descr):
        self.out_descr = out_descr
        self.line_number = line_number
#    def set_date_time(self, new_date_time):
#        self.date_time = new_date_time
#    def set_message(self, new_message):
#        self.message = new_message
    def parse_fields(self, line, format_t):
        print('format>',format_t['name'])
        fields_names = format_t['template'].split(' ')
        fields = re.findall(format_t['regexp'], line)
        if len(fields) == 0 or len(fields[0]) != len(fields_names):
            raise FormatTemplateError()
        self.fields = {}
        for field_id, field in enumerate(fields_names):
            self.fields[field] = fields[0][field_id]

    def parse_date_time(self, time_zone, custom_date_time=None):
        #datetime formats:
        #2017-05-12T07:36:00.065548Z
        #2017-05-12 07:35:59.929+0000
        #2017-05-12 03:26:25,540-0400
        #2017-05-12 03:23:31,135-04
        #2017-05-12 03:26:22,349
        #2017-05-12 03:28:13
        if custom_date_time is not None:
            dt = custom_date_time
        else:
            dt = self.fields['date_time']
        if dt == '':
            raise DateTimeNotFoundError()
        dt = dt.replace('T', ' ')
        dt = dt.replace('Z', '+0000')
        dt = dt.replace('.', ',')
        if ('+' in dt.partition(' ')[2] and len(dt.partition('+')[2]) < 4) \
            or ('-' in dt.partition(' ')[2] and \
                len((dt.partition(' ')[2]).partition('-')[2]) < 4):
            dt += '00'
        dt_formats = ["%Y-%m-%d %H:%M:%S,%f%z", \
                        "%Y-%m-%d %H:%M:%S%z", \
                        "%Y-%m-%d %H:%M:%S,%f", \
                        "%Y-%m-%d %H:%M:%S"]
        for dt_format in dt_formats:
            try:
                date_time = datetime.strptime(
                    dt, dt_format)
                if '%z' in dt_format:
                    self.fields['date_time'] = str(date_time.astimezone(pytz.utc))
                else:
                    date_time = date_time.replace(tzinfo=time_zone)
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
            r"[^^][^\ \t\n\,]+=[^\ \t\n\,]+.+|[^^]\"[^\"]{20,}\"|"+\
            r"[^^]\'[^\']{20,}\'|[^^]\[+.{20,}\]+|"+\
            r"[^^]\(+.{20,}\)+|[^^]\{+.{20,}\}+|[^^]\<+.{20,}\>+|"+\
            r"[^^][^\ \t\,\.\;\:]{20,}|[^^][\d\.\:]{10,}|"+\
            r"\[+[\d]*\]+|\(+[\d]*\)+|\{+[\d]*\}+|\<+[\d]*\>+")
        mstext = re.sub(template, '<...>', mstext)
        mstext = re.sub(re.compile(r"((\<\.\.\.\>){2,})"), '<...>', mstext)
        self.filtered_message = re.sub(r'^[\ \t\.\,\:\=]+|[\ \t\.\,\n]+$', '', mstext)

def loop_over_lines(logname, format_template, time_zome, out_descr):
    file_lines = {}
    fields_names = format_template['template'].split(' ')
    fields_names.remove('message')
    fields_names.remove('date_time')
    for field_name in fields_names:
        file_lines[field_name] = {}
    file_lines['filtered_message'] = {}

    prog = re.compile(format_template['regexp'])
    with open(logname) as f:
        prev_fields = {}
        prev_message = ''
        in_traceback_line = ''
        in_traceback_flag = False
        for line_num, line in enumerate(f):
            line_data = LogLine(line_num, out_descr)
            try:
                line_data.parse_fields(line, format_template)
                line_data.parse_date_time(time_zome)
                line_data.parse_message()
                fields = line_data.fields
                filtered_message = line_data.filtered_message
                if in_traceback_flag:
                    for field in sorted(fields_names):
                        if prev_fields[field] not in file_lines[field].keys():
                            file_lines[field][prev_fields[field]] = {}
                            file_lines[field][prev_fields[field]]['date_time'] = [prev_fields['date_time']]
                            file_lines[field][prev_fields[field]]['line_number'] = [prev_line_number]
                        else:
                            file_lines[field][prev_fields[field]]['date_time'] += [prev_fields['date_time']]
                            file_lines[field][prev_fields[field]]['line_number'] += [prev_line_number]
                        #file_lines[field] += [prev_fields[field]]
                    if prev_message+'; '+in_traceback_line not in file_lines['filtered_message'].keys():
                        file_lines['filtered_message'][prev_message+'; '+in_traceback_line] = {}
                        file_lines['filtered_message'][prev_message+'; '+in_traceback_line]['date_time'] = [prev_fields['date_time']]
                        file_lines['filtered_message'][prev_message+'; '+in_traceback_line]['line_number'] = [prev_line_number]
                    else:
                        print('I have such message!')
                        file_lines['filtered_message'][prev_message+'; '+in_traceback_line]['date_time'] += [prev_fields['date_time']]
                        file_lines['filtered_message'][prev_message+'; '+in_traceback_line]['line_number'] += [prev_line_number]
                    #file_lines['filtered_message'] += [prev_message+'; '+\
                    #                            in_traceback_line]
                    #file_lines['line_number'] += [prev_line_number]
                    #print(prev_message)
                    #print()
                    in_traceback_flag = False
                elif prev_message != '':
                    for field in sorted(fields_names):
                        if prev_fields[field] not in file_lines[field].keys():
                            file_lines[field][prev_fields[field]] = {}
                            file_lines[field][prev_fields[field]]['date_time'] = [prev_fields['date_time']]
                            file_lines[field][prev_fields[field]]['line_number'] = [prev_line_number]
                        else:
                            file_lines[field][prev_fields[field]]['date_time'] += [prev_fields['date_time']]
                            file_lines[field][prev_fields[field]]['line_number'] += [prev_line_number]
                        #file_lines[field] += [prev_fields[field]]
                    if prev_message not in file_lines['filtered_message'].keys():
                        file_lines['filtered_message'][prev_message] = {}
                        file_lines['filtered_message'][prev_message]['date_time'] = [prev_fields['date_time']]
                        file_lines['filtered_message'][prev_message]['line_number'] = [prev_line_number]
                    else:
                        print('I have such message!')
                        file_lines['filtered_message'][prev_message]['date_time'] += [prev_fields['date_time']]
                        file_lines['filtered_message'][prev_message]['line_number'] += [prev_line_number]
                    #for field in sorted(fields_names):
                    #    file_lines[field] += [prev_fields[field]]
                    #file_lines['filtered_message'] += [prev_message]
                    #file_lines['line_number'] += [prev_line_number]
                    #print(prev_message)
                    #print()
                prev_fields = fields
                prev_message = filtered_message
                prev_line_number = line_num
            except FormatTemplateError as exception_message:
                if 'Traceback' in line:
                    in_traceback_flag = True
                elif in_traceback_flag:
                    mess = LogLine(line_num, out_descr)
                    mess.parse_message(line)
                    in_traceback_line = mess.filtered_message
                else:
                    out_descr.write('Warning: parse_fields: Line does not match '+\
                        'format "%s": %s\n'% (format_template['name'], line))
                    if '!Fake datetime!' in prev_message:
                        mess = LogLine(line_num, out_descr)
                        mess.parse_message(line)
                        prev_message += mess.filtered_message
                        prev_message = re.sub(re.compile(r"((\<\.\.\.\>){2,})"), '<...>', prev_message)
                    else:
                        mess = LogLine(line_num, out_descr)
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
        for field in sorted(fields_names):
            if prev_fields[field] not in file_lines[field].keys():
                file_lines[field][prev_fields[field]] = {}
                file_lines[field][prev_fields[field]]['date_time'] = [prev_fields['date_time']]
                file_lines[field][prev_fields[field]]['line_number'] = [prev_line_number]
            else:
                file_lines[field][prev_fields[field]]['date_time'] += [prev_fields['date_time']]
                file_lines[field][prev_fields[field]]['line_number'] += [prev_line_number]
            #file_lines[field] += [prev_fields[field]]
        if in_traceback_flag:
            if prev_message+'; '+in_traceback_line not in file_lines['filtered_message'].keys():
                file_lines['filtered_message'][prev_message+'; '+in_traceback_line] = {}
                file_lines['filtered_message'][prev_message+'; '+in_traceback_line]['date_time'] = [prev_fields['date_time']]
                file_lines['filtered_message'][prev_message+'; '+in_traceback_line]['line_number'] = [prev_line_number]
            else:
                print('I have such message!')
                file_lines['filtered_message'][prev_message+'; '+in_traceback_line]['date_time'] += [prev_fields['date_time']]
                file_lines['filtered_message'][prev_message+'; '+in_traceback_line]['line_number'] += [prev_line_number]
            in_traceback_flag = False
        else:
            if prev_message not in file_lines['filtered_message'].keys():
                file_lines['filtered_message'][prev_message] = {}
                file_lines['filtered_message'][prev_message]['date_time'] = [prev_fields['date_time']]
                file_lines['filtered_message'][prev_message]['line_number'] = [prev_line_number]
            else:
                print('I have such message!')
                file_lines['filtered_message'][prev_message]['date_time'] += [prev_fields['date_time']]
                file_lines['filtered_message'][prev_message]['line_number'] += [prev_line_number]
        #file_lines['line_number'] += [prev_line_number]

    print('++++++++++++++++++++++++')
    f = open("lines.js", 'w')
    json.dump(file_lines, f, indent=4, sort_keys=True)
    f.close()
    return file_lines
