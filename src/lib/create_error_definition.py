"""Parsing error messages from logfile
    - loop_over_lines - creates lists with errors' information from one log file, \
     including information in tracebacks' messages
    - Class LogLine - represents information about one error (datetime, \
        sender, thread, event, message)
"""
import numpy as np
import inspect
import pytz
import re
from datetime import datetime


class LogLine:
    #date_time = 0
    #thread = ''
    #who_send = ''
    #event = ''
    #message = ''

    def __init__(self, line, format_template, out_descr):
        self.raw_text = line
        self.out_descr = out_descr
        self.format = format_template

    def parse_date_time(self):
        dt_next_symbol = self.format.partition("datetime")[1][0]
        print(">>>", dt_next_symbol)
        dt = self.raw_text.partition(" ERROR ")[0]
        if dt.partition(',')[2][3:] == 'Z':
            dt = dt.replace('Z', '+0000')
        elif len(dt.partition(',')[2][3:]) == 3:
            dt = dt + '00'
        try:
            self.date_time = datetime.strptime(
                dt, "%Y-%m-%d %H:%M:%S,%f%z").replace(tzinfo=pytz.utc)
            #self.out_descr.write('Time: %s' % self.date_time)
            return True
        except ValueError:
            self.out_descr.write('Unknown format: %s\n' % dt)
            return False

    def parse_sender(self):
        template = re.compile(r"\[(.*?)\]")
        t = re.search(template, self.raw_text.partition(" ERROR ")[2])
        if t is not None:
            self.who_send = t.group(1)
            #self.out_descr.write('Sender: %s' % t.group(1))
            return True
        else:
            self.out_descr.write('Sender was not found: %s\n' %
                                 self.raw_text.partition(" ERROR ")[2])
            return False

    def parse_thread(self):
        template = re.compile(r"\((.*?)\)")
        t = re.search(template, self.raw_text.partition(" ERROR ")[2])
        if t:
            self.thread = t.group(1)
            #self.out_descr.write('Thread: %s ' % t.group(1))
            return True
        else:
            self.out_descr.write('Thread was not found: %s\n' %
                                 self.raw_text.partition(" ERROR ")[2])
            return False

    def parse_message(self):
        if ' ERROR ' in self.raw_text:
            mstext = self.raw_text.partition(" ERROR ")[2]
        else:
            mstext = self.raw_text
        template = re.compile(r"Message: (.*)")
        t = re.search(template, mstext)
        if t is not None and len(t.group(1)) > 5:
            t_mes = re.split(r":", t.group(1))
            if len(t_mes) > 1:
                self.event = re.sub(r"\..*", '', t_mes[0])
                self.message = [re.sub(r'\"', '', mes_i)
                                for mes_i in t_mes[1:]]
            else:
                t_mes_underlying = re.split(r"message", t_mes[0])
                if len(t_mes_underlying) > 1:
                    self.event = t_mes_underlying[0]
                    self.message = [re.sub(r'\"', '', mes_i)
                                    for mes_i in t_mes_underlying[1:]]
                else:
                    self.event = 'Unknown'
                    self.message = [re.sub(r'\"', '', t_mes[0])]
            #self.out_descr.write('Event = %s' % self.event)
            #self.out_descr.write('MESSAGE = %s' % self.message)
            return True
        else:
            t_mes = re.split(r"message", mstext)
            if len(t_mes) == 2:
                self.message = [re.sub(r'[:\{\}\'\n\"]', '', t_mes[1])]
                self.event = 'Unknown'
                #self.out_descr.write('Event = %s' % self.event)
                #self.out_descr.write('Message = %s' % self.message)
                return True
            template = re.compile(r"(\[.*?\]|\(.*?\)|\{.*?\}|\<.*?\>) *")
            t = re.sub(template, '', mstext)
            t = re.sub('\n', '', t)
            if t is not None and len(t) > 5:
                t_mes = re.split(r":", t)
                if len(t_mes) > 1:
                    self.event = t_mes[0]
                    self.message = [re.sub(r'\"', '', mes_i)
                                    for mes_i in t_mes[1:]]
                else:
                    t_mes_underlying = re.split(r"message", t_mes[0])
                    if len(t_mes_underlying) > 1:
                        self.event = t_mes_underlying[0]
                        self.message = [re.sub(r'\"', '', mes_i)
                                        for mes_i in t_mes_underlying[1:]]
                    else:
                        self.event = 'Unknown'
                        self.message = [re.sub(r'\"', '', t_mes[0])]
                #self.out_descr.write('Event = %s' % self.event)
                #self.out_descr.write('Message = %s' % self.message)
                return True
            else:
                self.message = re.sub(r"\n", '', re.sub(
                    r"\[(.*?)\]", '', re.sub(r"\((.*?)\)", '', mstext)))
                self.event = 'Unknown'
                self.out_descr.write('Message was not found: >> %s' %
                                     re.sub(r"\n", '', mstext) + ' <<\n')
                #self.out_descr.write('Event = %s' % self.event)
                #self.out_descr.write('Message = %s' % self.message)
                return False

def loop_over_lines(logname, file_format, out_descr):
    #error_datetime = []
    #error_msg_text = []
    #error_who_send = []
    #error_event = []
    #error_thread = []
    #print(file_format)
    file_lines = {}
    for entity in file_format['template']:
        file_lines[entity] = []

    with open(logname) as f:
        error_info = {}
        error_traceback = ''
        in_traceback_flag = False
        for line in f:
            if any(w in line for w in [' INFO ',' DEBUG ',' ERROR ',' WARN ']):
                if error_info:
                    #self.out_descr.write('%s\n%s\n' % (line, error_info))
                    error_datetime += [error_info['datetime']]
                    error_who_send += [error_info['who_send']]
                    error_thread += [error_info['thread']]
                    if not in_traceback_flag:
                        error_event += [error_info['event']]
                        error_msg_text += [error_info['msg_text']]
                    else:
                        traceback_message = LogLine(
                            error_traceback, file_format, out_descr)
                        traceback_message.parse_message()
                        error_event += [traceback_message.event]
                        error_msg_text += [traceback_message.message]
                error_info = {}
                error_traceback = ''
                in_traceback_flag = False
                if ' ERROR ' in line:
                    e = LogLine(line, file_format, out_descr)
                    error_attributes = []
                    for (method_name, method) in inspect.getmembers(
                                        LogLine, predicate=inspect.isfunction):
                        if 'parse' in method_name:
                            error_attributes += [method(e)]
                    if not all(error_attributes):
                        continue
                    error_info['datetime'] = int(
                        e.date_time.timestamp() * 1000)
                    error_info['who_send'] = e.who_send
                    error_info['thread'] = e.thread
                    error_info['event'] = e.event
                    error_info['msg_text'] = e.message
            elif 'Traceback' in line:
                in_traceback_flag = True
                error_traceback = line
            elif in_traceback_flag and not line == '':
                error_traceback = line
        # adding the last line
        if ' ERROR ' in line or in_traceback_flag:
            error_datetime += [error_info['datetime']]
            error_who_send += [error_info['who_send']]
            error_thread += [error_info['thread']]
            if not in_traceback_flag:
                error_event += [error_info['event']]
                error_msg_text += [error_info['msg_text']]
            else:
                error_event += [error_traceback['event']]
                error_msg_text += [error_traceback['msg_text']]
    return error_datetime, error_who_send, error_thread, error_event, \
                error_msg_text
