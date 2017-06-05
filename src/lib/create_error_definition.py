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
    #date_time = 0
    #message = ''
    #...
    def __init__(self, line, out_descr):
        self.raw_text = line
        self.out_descr = out_descr
    def set_date_time(self, new_date_time):
        self.date_time = new_date_time
    def set_message(self, new_message):
        self.message = new_message
    def parse_fields(self, format_t):
        fields_names = format_t['template'].split(' ')
        date_time_index = fields_names.index("date_time")
        message_index = fields_names.index("message")
        fields = re.findall(format_t['regexp'], self.raw_text)
        if len(fields) == 0:
            raise FormatTemplateError(\
                'parse_fields: Line does not match format "%s":\n%s\n'\
                    % (format_t['name'], self.raw_text))
        self.date_time = fields[0][date_time_index]
        self.message = fields[0][message_index]

    def parse_date_time(self, time_zone = pytz.utc):
        #datetime formats:
        #2017-05-12T07:36:00.065548Z
        #2017-05-12 07:35:59.929+0000
        #2017-05-12 03:26:25,540-0400
        #2017-05-12 03:23:31,135-04
        #2017-05-12 03:26:22,349
        #2017-05-12 03:28:13

        dt = self.date_time
        if dt == '':
            raise DateTimeNotFoundError(\
                'parse_date_time: Line does not have date_time field:\n%s\n'\
                    % self.raw_text)
        dt = dt.replace('T', ' ')
        dt = dt.replace('Z', '+0000')
        dt = dt.replace('.', ',')
        if ('+' in dt.partition(' ')[2] and len(dt.partition('+')[2]) < 4) \
            or ('-' in dt.partition(' ')[2] and \
                len((dt.partition(' ')[2]).partition('-')[2]) < 4):
            dt += '00'

        dt_formats = ["%Y-%m-%d %H:%M:%S,%f%z", \
                        "%Y-%m-%d %H:%M:%S,%f", \
                        "%Y-%m-%d %H:%M:%S"]
        for dt_format in dt_formats:
            try:
                date_time = datetime.strptime(
                    dt, dt_format)
                if dt_formats == 0:
                    date_time = date_time.astimezone(pytz.utc)
                else:
                    date_time = date_time.replace(tzinfo=time_zone)
                    date_time = date_time.astimezone(pytz.utc)
                #self.out_descr.write('Time: %s\n' % date_time)
                return date_time
            except ValueError:
                continue
        raise DateTimeFormatError('Unknown date_time format: %s\n' % dt)

    def parse_message(self, ms_line=None):
        if ms_line is not None:
            mstext = ms_line
        else:
            mstext = self.message
        if mstext == '':
            raise MessageNotFoundError('parse_message: \
                Line does not have message '+'field:\n%s\n' % self.raw_text)
        #template = re.compile(r'([Ee]rr[or]*|[Mm]essage)[\:\=\ \'\"\>]+(.+)')
        template = re.compile(r'([Mm]essage[\:\=\ \'\"]+)(.+)')
        t = re.search(template, mstext)
        if t is not None:
            mstext = t.group(2)
        template = re.compile(\
            r"[^^]\"[^\"]{30,}\"|[^^]\'[^\']{30,}\'|[^^]\[+.{30,}\]+|"+
            r"[^^]\(+.{30,}\)+|[^^]\{+.{30,}\}+|[^^]\<+.{30,}\>+|"+
            r"[^^][\w\-]{30,}")
        mstext = re.sub(template, '<...>', mstext)
        message = re.sub(r'^[\ \:\=]+|[\ \.\n]+$', '', mstext)
        #print(self.message)
        return message

    def parse_other_fields(self, format_t):
        pass

#    def parse_date_time(self):
#        dt_next_symbol = self.format.partition("datetime")[1][0]
#        print(">>>", dt_next_symbol)
#        dt = self.raw_text.partition(" ERROR ")[0]
#        if dt.partition(',')[2][3:] == 'Z':
#            dt = dt.replace('Z', '+0000')
#        elif len(dt.partition(',')[2][3:]) == 3:
#            dt = dt + '00'
#        try:
#            self.date_time = datetime.strptime(
#                dt, "%Y-%m-%d %H:%M:%S,%f%z").replace(tzinfo=pytz.utc)
#            #self.out_descr.write('Time: %s' % self.date_time)
#            return True
#        except ValueError:
#            self.out_descr.write('Unknown format: %s\n' % dt)
#            return False

def loop_over_lines(logname, format_template, out_descr):
    file_lines = {}
    fields_names = format_template['template'].split(' ')
    for field_name in fields_names:
        file_lines[field_name] = []

    prog = re.compile(format_template['regexp'])
    with open(logname) as f:
        prev_date_time = ''
        prev_message = ''
        in_traceback_line = ''
        in_traceback_flag = False
        for line in f:
            line_data = LogLine(line, out_descr)
            try:
                line_data.parse_fields(format_template)
                date_time = line_data.parse_date_time()
                message = line_data.parse_message()
                if in_traceback_flag:
                    print('quit from traceback')
                    print(prev_date_time)
                    print(prev_message+'; '+in_traceback_line+'\n')
                    file_lines['date_time'] += [prev_date_time]
                    file_lines['message'] += [prev_message+'; '+\
                                                in_traceback_line]
                    in_traceback_flag = False
                elif prev_date_time != '':
                    print(prev_date_time)
                    print(prev_message+'\n')
                    file_lines['date_time'] += [prev_date_time]
                    file_lines['message'] += [prev_message]
                prev_date_time = date_time
                prev_message = message
            except FormatTemplateError as exception_message:
                #print(str(exception_message))
                if 'Traceback' in line:
                    #print('Traceback')
                    in_traceback_flag = True
                elif in_traceback_flag:
                    in_traceback_line = LogLine(line, \
                                            out_descr).parse_message(line)
                else:
                    #print('Date+time was set to ', date_time)
                    #print('another format')
                    #print(prev_date_time)
                    #print(prev_message+'\n')
                    #file_lines['date_time'] += [prev_date_time]
                    #file_lines['message'] += [prev_message]
                    if 'Fake datetime' in prev_message:
                        prev_message += LogLine(line, \
                                            out_descr).parse_message(line)
                    else:
                        prev_message = '!Fake datetime! ' + \
                                    LogLine(line, out_descr).parse_message(line)
            except DateTimeNotFoundError as exception_message:
                pass
            except DateTimeFormatError as exception_message:
                pass
            except MessageNotFoundError as exception_message:
                pass
        file_lines['date_time'] += [prev_date_time]
        if in_traceback_flag:
            file_lines['message'] += [prev_message+'; '+in_traceback_line]
            in_traceback_flag = False
        else:
            file_lines['message'] += [prev_message]
        print(prev_date_time)
        print(prev_message+'\n')

    #print('++++++++++++++++++++++++')
    #print(file_lines)
    return file_lines
