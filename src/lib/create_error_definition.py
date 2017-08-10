"""Parsing error messages from logfile
- Class LogLine - represents information about one error (datetime,
sender, thread, event, message)
"""
import pytz
import lzma
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


class DateTimeGreaterTimeRanges(LogLineError):
    """Raised when datetime is greater all defined time ranges (there is no
    need to continue parsing lines)"""
    pass


class LogLine:
    msg_template = re.compile(r'(Message[\:\=\ ]+.+)')
    re_timestamp = re.compile(
        r"[0-9\-]{10}[\sT][0-9]{2}:[0-9]{2}:[0-9]{2}[\.\,0-9]*[\+\-0-9Z]*")
    re_punctuation = re.compile(r'^[\ \t\.\,\:\=]+|[\ \t\.\,\n]+$')
    dt_formats = ["%Y-%m-%d %H:%M:%S,%f%z", "%Y-%m-%d %H:%M:%S%z"]

    def __init__(self, fields_names, line_num, out_descr, time_ranges):
        self.out_descr = out_descr
        self.time_ranges = time_ranges
        self.raw_line = ''
        self.fields = {}
        for field in fields_names:
            self.fields[field] = ''
        self.fields['line_num'] = line_num

    def parse_date_time(self, time_zone, line):
        # datetime formats:
        # 2017-05-12T07:36:00.065548Z
        # 2017-05-12 07:35:59.929+0000
        # 2017-05-12 03:26:25,540-0400
        # 2017-05-12 03:23:31,135-04
        # 2017-05-12 03:26:22,349
        # 2017-05-12 03:28:13
        self.raw_line = line
        match = self.re_timestamp.search(line)
        if match is None or line[0] in [' ', '\t']:
            raise DateTimeNotFoundError(
                'Warning: parse_date_time: ' +
                'Line does not have date_time field: %s\n' % line)
        dt = match.group(0)
        dt = dt.replace('T', ' ')
        dt = dt.replace('Z', '+0000')
        dt = dt.replace('.', ',')
        time_part = dt.partition(' ')[2]
        # for "2017-05-12 03:23:31,135-04" format
        if (any([sign in time_part
                 and len(time_part.partition(sign)[2]) == 2
                 for sign in ['+', '-']])):
            dt += '00'
        elif not any([sign in time_part for sign in ['+', '-']]):
            # if we have time without time zone
            dt += time_zone
        for dt_format in self.dt_formats:
            try:
                date_time = datetime.strptime(dt, dt_format)
                date_time = date_time.astimezone(pytz.utc)
                self.fields['date_time'] = date_time.timestamp()
                break
                # self.out_descr.put('Time: %s\n' % date_time)
            except ValueError:
                continue
        if self.fields['date_time'] == '':
            raise DateTimeFormatError("Warning: parse_date_time: " +
                                      "Unknown date_time format: " +
                                      "%s\n" % dt)
        # Check user-defined time range
        if self.time_ranges != []:
            if all([self.fields['date_time'] > tr[1]
                    for tr in self.time_ranges]):
                raise DateTimeGreaterTimeRanges()
            if not any([self.fields['date_time'] >= tr[0] and
                        self.fields['date_time'] <= tr[1]
                        for tr in self.time_ranges]):
                raise DateTimeNotInTimeRange()

    def parse_fields(self, pattern, line):
        line = line.strip()
        fields = pattern.search(line)
        if fields is None:
            raise FormatTemplateError()
        fields = fields.groupdict()
        if self.fields["date_time"] == '':
            pass  # STH WRONG
        for field in sorted(fields.keys()):
            if field == "date_time":
                continue
            self.fields[field] = fields[field]

    def parse_message(self, custom_message_text=None):
        if custom_message_text is not None:
            mstext = custom_message_text
        else:
            mstext = self.fields['message']
        if mstext == '':
            raise MessageNotFoundError()
        t = self.msg_template.search(mstext)
        if t is not None:
            mstext = t.group(1)
        self.fields["message"] = self.re_punctuation.sub('', mstext)


def check_constraints(line, events, host_ids, vm_numbers, tasks, flow_ids):
    if any([re.search(r'(^|[ \:\.\,]+)' + keyword + r'([ \:\.\,=]+|$)',
            line.lower()) is not None for keyword in events + host_ids +
            vm_numbers + ['error', 'fail', 'failure', 'failed', 'traceback',
                          'warn', 'warning', 'exception', 'down',
                          'crash']]):
        return True
    if any([thread in line for thread in tasks]):
        return True
    if any(['flow_id='+flow in line for flow in flow_ids]):
        return True
    else:
        return False


def create_line_info(in_traceback_flag, in_traceback_line, multiline_flag,
                     multiline_line, fields_names, out_descr, time_zone,
                     time_ranges, events, host_ids, vm_numbers,
                     format_template, prev_fields, prev_line, tasks, flow_ids,
                     show_warnings):
    # write a concatenated string that include a Traceback message (try to
    # match the template first (if there were any fields that appear in the
    # other lines)
    if in_traceback_flag:
        in_traceback_flag = False
        prev_line = prev_line + in_traceback_line
        # check if the line satisfy user conditions
        if not check_constraints(prev_line, events, host_ids, vm_numbers,
                                 tasks, flow_ids):
            return prev_line, [], in_traceback_flag, multiline_flag
        try:
            # try to match with the log file format template
            mess = LogLine(fields_names, prev_fields['line_num'],
                           out_descr, time_ranges)
            # receive a more clear message
            mess.parse_message(prev_fields["message"] + ' ' +
                               in_traceback_line)
            # save the previous line
            line_info = []
            for field in fields_names:
                if field == 'message':
                    line_info += [mess.fields["message"]]
                    continue
                line_info += [prev_fields[field]]
            # line_info += [mess.fields["message"]]
            if show_warnings:
                out_descr.put('Info: create_line_info: ' +
                              'Traceback matched: %s\n' %
                              in_traceback_line)
            return prev_line, line_info, in_traceback_flag, multiline_flag
        # if message is empty
        except MessageNotFoundError:
            if show_warnings:
                out_descr.put('Warning: parse_message: ' +
                              'Line does not have message field: %s\n' %
                              prev_fields["message"] +
                              ' '+in_traceback_line)
            return prev_line, [], in_traceback_flag, multiline_flag
    # write a concatenated string that include a multiline message (try to
    # match the template first (if there were any fields that appear in the
    # other lines)
    elif multiline_flag:
        multiline_flag = False
        prev_line = multiline_line
        # check if the line satisfy user conditions
        if not check_constraints(prev_line, events, host_ids, vm_numbers,
                                 tasks, flow_ids):
            return prev_line, [], in_traceback_flag, multiline_flag
        try:
            # try to match with the log file format template
            mess = LogLine(fields_names,
                           prev_fields['line_num'].split(':')[0] + ':' +
                           str(int(prev_fields['line_num'].split(':')[1]) + 1),
                           out_descr, time_ranges)
            mess.parse_date_time(time_zone, multiline_line)
            mess.parse_fields(format_template, multiline_line)
            mess.parse_message()
            line_info = []
            for field in fields_names:
                line_info += [mess.fields[field]]
            # line_info += [mess.fields["message"]]
            if show_warnings:
                out_descr.put('Info: create_line_info: ' +
                              'Multiline matched: %s\n' %
                              multiline_line)
            return prev_line, line_info, in_traceback_flag, multiline_flag
        except DateTimeNotInTimeRange:
            return prev_line, [], in_traceback_flag, multiline_flag
        except DateTimeGreaterTimeRanges:
            return prev_line, [], in_traceback_flag, multiline_flag
        except (DateTimeNotFoundError, DateTimeFormatError) as \
                exception_message:
            if show_warnings:
                out_descr.put(str(exception_message))
            line_info = []
            for field in fields_names:
                line_info += [prev_fields[field]]
            line_info += [multiline_line]
            return prev_line, line_info, in_traceback_flag, multiline_flag
        except FormatTemplateError:
            if show_warnings:
                out_descr.put('Warning: parse_fields: ' +
                              'Line does not match format "%s": %s\n' %
                              (format_template, multiline_line))
            line_info = []
            for field in fields_names:
                line_info += [prev_fields[field]]
            line_info += ['!Fake datetime! ' + multiline_line]
            return prev_line, line_info, in_traceback_flag, multiline_flag
        except MessageNotFoundError:
            if show_warnings:
                out_descr.put('Warning: parse_message: ' +
                              'Line does not have message field: %s\n' %
                              prev_line)
            return prev_line, [], in_traceback_flag, multiline_flag
    # that was a normal line, check used constraints and save
    else:
        # check if the line satisfy user conditions
        if not check_constraints(prev_line, events, host_ids, vm_numbers,
                                 tasks, flow_ids):
            return prev_line, [], in_traceback_flag, multiline_flag
        line_info = []
        for field in fields_names:
            line_info += [prev_fields[field]]
        return prev_line, line_info, in_traceback_flag, multiline_flag


def loop_over_lines(directory, logname, format_template, time_zone, first_line,
                    out_descr, events, host_ids, time_ranges, vm_numbers,
                    tasks, flow_ids, show_warnings, progressbar=None):
    full_filename = os.path.join(directory, logname)
    # format_template = re.compile(format_template)
    fields_names = list(sorted(format_template.groupindex.keys()))
    fields_names.remove("message")
    fields_names.remove("date_time")
    fields_names = ['date_time', 'line_num', 'message'] + fields_names
    # out = open('result_'+logname+'.txt', 'w')
    file_lines = []
    if first_line == -1:
        # file is not in time range
        return file_lines, fields_names
    if logname[-4:] == '.log':
        f = open(full_filename)
    elif logname[-3:] == '.xz':
        f = lzma.open(full_filename, 'rt')
    f.seek(0, os.SEEK_END)
    num = f.tell()
    f.seek(0, 0)
    prev_fields = {}
    in_traceback_line = ''
    in_traceback_flag = False
    multiline_line = ''
    multiline_flag = False
    store = False
    if progressbar:
        progressbar.start(max_value=num)
    count = 0
    prev_line = ''
    line = ''
    regexp = r"^([\ \x00\t]*)$"
    if 'libvirt' in logname:
        regexp = regexp + r".*|OBJECT_|.*release\ domain"
    re_skip = re.compile(regexp)
    for line_num, line in enumerate(f):
        # if line is empty and other cases when we don't need to parse it
        if (line_num < first_line or re_skip.match(line) is not None):
            count += len(line)
            if progressbar:
                progressbar.update(count)
            continue
        line_data = LogLine(fields_names, logname+':'+str(line_num+1),
                            out_descr, time_ranges)
        try:
            line_data.parse_date_time(time_zone, line)
            line_data.parse_fields(format_template, line)
            line_data.parse_message()
            # succesfully parsed the line => we need to save the previous line,
            # it might be with a Traceback of other non-standard cases
            # store mean that previos line should te saved for analysis (it
            # satisfy the used constraints)
            if store and prev_fields != {}:
                prev_line, line_info, in_traceback_flag, multiline_flag = \
                    create_line_info(in_traceback_flag,
                                     in_traceback_line, multiline_flag,
                                     multiline_line, fields_names, out_descr,
                                     time_zone, time_ranges, events, host_ids,
                                     vm_numbers, format_template,
                                     prev_fields, prev_line, tasks, flow_ids,
                                     show_warnings)
                # if we normally parsed the previous line, we save it
                if line_info != []:
                    file_lines += [line_info]
            # we saved if it was nessesary the previous line, the current
            # became the previous
            prev_fields = line_data.fields
            prev_line = line
            store = True
        # if the date time in out of the defined time ranges (store became
        # False)
        except DateTimeNotInTimeRange:
            if store and prev_fields != {}:
                prev_line, line_info, in_traceback_flag, multiline_flag = \
                    create_line_info(in_traceback_flag,
                                     in_traceback_line, multiline_flag,
                                     multiline_line, fields_names, out_descr,
                                     time_zone, time_ranges, events, host_ids,
                                     vm_numbers, format_template,
                                     prev_fields, prev_line, tasks, flow_ids,
                                     show_warnings)
                if line_info != []:
                    file_lines += [line_info]
            store = False
        # if the date time is treater all the user-defined time ranges, we can
        # stop parsing
        except DateTimeGreaterTimeRanges:
            if store and prev_fields != {}:
                prev_line, line_info, in_traceback_flag, multiline_flag = \
                    create_line_info(in_traceback_flag,
                                     in_traceback_line, multiline_flag,
                                     multiline_line, fields_names, out_descr,
                                     time_zone, time_ranges, events, host_ids,
                                     vm_numbers, format_template,
                                     prev_fields, prev_line, tasks, flow_ids,
                                     show_warnings)
                if line_info != []:
                    file_lines += [line_info]
            count = num
            if progressbar:
                progressbar.update(count)
            break
        # if the parser didn't find the date time
        except (DateTimeNotFoundError, DateTimeFormatError) as \
                exception_message:
            if not store:
                pass
            elif prev_fields == {}:
                if show_warnings:
                    out_descr.put(str(line_num+1) + ': ')
                    out_descr.put(str(exception_message))
            elif in_traceback_flag:
                # Remember a line if we are in a traceback.
                # The line will be concatenated with a message
                line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\x00\n]+$', ' ', line)
                in_traceback_line += line
            elif multiline_flag:
                line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\x00\n]+$', ' ', line)
                multiline_line += line
            elif 'Traceback' in line or re.match(
                        r'^[\t\ ]*[at,Caused,\.\.\.].+', line) is not None:
                # We are in a traceback
                in_traceback_flag = True
                line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\x00\n]+$', ' ', line)
                in_traceback_line = line
            else:
                # The analyzer didn't find a datetime in a line,
                # the message will receive datetime from previous
                # message with a mark "Fake datetime"
                if show_warnings:
                    out_descr.put(str(exception_message))
                multiline_flag = True
                line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\x00\n]+$', ' ', line)
                multiline_line = re.sub(
                    r'^[\t\ \.\,\=]+|[\t\ \.\,\x00\n]+$', ' ', prev_line) + \
                    line
        # if the line was not matched with the regex-format
        except FormatTemplateError:
            if show_warnings:
                out_descr.put('Warning: parse_fields: ' +
                              'Line does not match format "%s": %s\n' %
                              (format_template, line))
            # We are in a line with datetime, but the analyzer didn't
            # find all fields from a template
            if store and prev_fields != {}:
                prev_line, line_info, in_traceback_flag, multiline_flag = \
                    create_line_info(in_traceback_flag,
                                     in_traceback_line, multiline_flag,
                                     multiline_line, fields_names, out_descr,
                                     time_zone, time_ranges, events, host_ids,
                                     vm_numbers, format_template, prev_fields,
                                     prev_line, tasks, flow_ids,
                                     show_warnings)
                if line_info != []:
                    file_lines += [line_info]
            multiline_flag = True
            line = re.sub(r'^[\t\ \.\,\=]+|[\t\ \.\,\n]+$', '', line)
            multiline_line = line
        # if the message is empty
        except MessageNotFoundError:
            if show_warnings:
                out_descr.put('Warning: parse_message: ' +
                              'Line does not have message field: %s\n' % line)
        # for progressbar
        count += len(line)
        if progressbar:
            progressbar.update(count)

    # adding the last line
    if store and prev_fields != {}:
        prev_line, line_info, in_traceback_flag, multiline_flag = \
            create_line_info(in_traceback_flag,
                             in_traceback_line, multiline_flag,
                             multiline_line, fields_names, out_descr,
                             time_zone, time_ranges, events, host_ids,
                             vm_numbers, format_template, prev_fields,
                             prev_line, tasks, flow_ids, show_warnings)
        if line_info != []:
            file_lines += [line_info]
    f.close()
    if progressbar:
        progressbar.finish()
    return file_lines, fields_names  # + ['message']
