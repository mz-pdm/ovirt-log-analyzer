"""Saving received information about log lines
This module contains methods of saving errors' statistics
    - print_all_headers - Prints all information about a line (all fields that \
                        parser found from the lines according to the template)
    - print_only_dt_message - Prints only information about a line's \
                        date_time, line number from a log file and general \
                        message

    - Here will be new functions to detect dangerous and important messages
    
    - [will be used]dump_json - saves dictionary with errors' information \
        to .js file (time first). Used for creating interactive chart \
        ("oVirt_logErrors_statistics_by_time.html")
    - [not used]print_statistics - generates str with convenient for perception\
        output (logname, sender, event first; time or time range for each \
        error message)
"""
import numpy as np
import re, json
from datetime import datetime

def print_all_headers(errors, headers, log_format_headers, out):
    for err in errors:
        format_name = err[1].split(':')[0]
        for h in headers:
            if h in log_format_headers[format_name]:
                out.write("%s %s\n" % (h, err[log_format_headers[
                                                format_name].index(h)]))
            else:
                out.write("%s %s\n" % (h, ''))
        out.write("\n")

def print_only_dt_message(errors, out, headers):
    max_len = max(len(err[1]) for err in errors)
    dt_idx = headers.index("date_time")
    line_idx = headers.index("line_num")
    msg_idx = headers.index("message")
    for err in errors:
        out.write("%12s %s | %*s | %s\n" % (
            datetime.utcfromtimestamp(err[dt_idx]).strftime("%H:%M:%S,%f")[:-3],\
            datetime.utcfromtimestamp(err[dt_idx]).strftime("%d-%m-%Y"),\
            max_len, err[line_idx], err[msg_idx]))

#def dump_json(log_names, all_errors, filename, template):
#    html_text = open(template, 'r').read().split('\n')
#    all_data = {}
#    with open(filename + '.html', 'w') as outfile:
#        for log_idx, logname in enumerate(log_names):
#            logname = re.sub('[+\-\s,]', '_', logname)
#            all_data[logname] = all_errors[log_idx]
#        #json.dump(all_data, outfile, indent=4, sort_keys=True)
#        comment_flag = False
#        for line_idx, line in enumerate(html_text):
#            if '-->' in line and comment_flag:
#                comment_flag = False
#                continue
#            elif comment_flag:
#                continue
#            elif '<!--' in line:
#                comment_flag = True
#                continue
#            elif '</script>' in line and line_idx > 50:
#                outfile.write('var errors_data = \n')
#                outfile.write(json.dumps(all_data, indent=4, sort_keys=True))
#                outfile.write(';\n')
#                outfile.write(line + '\n')
#            else:
#                outfile.write(line + '\n')
#        outfile.close()

#def print_statistics(logfile_name, errors_dict, errors_time, errors_num):
#    err_info = '________________________________________\n\n'
#    err_info += "Summarized statistics for " + logfile_name + '\n'
#    err_info += "Number of error messages: " + str(errors_num) + '\n'
#    err_info += '________________________________________\n'
#    sum_all = []
#    all_time = []
#    for sender_id, sender in enumerate(sorted(errors_dict.keys())):
#        sum_sender = []
#        time_sender = []
#        for event_id, event in enumerate(sorted(errors_dict[sender].keys())):
#            sum_event = 0
#            time_event = []
#            for message_id, message in enumerate(sorted(errors_dict[sender][
#                                                                event].keys())):
#                sum_event += errors_dict[sender][event][message]
#                time_event += [errors_time[sender][event][message]]
#            sum_sender += [sum_event]
#            time_sender += [time_event]
#        sum_all += [sum_sender]
#        all_time += [time_sender]
#
#    for sender_id, sender in enumerate(sorted(errors_dict.keys())):
#        if np.sum(sum_all[sender_id]) < 3:
#            str_time = ''.join([datetime.utcfromtimestamp(m_n / 1000).strftime(
#                "%H-%M-%S.%f %d.%m.%Y") + '; ' for r in all_time[sender_id] \
#                        for m in r for m_n in m])
#        else:
#            str_time = min([datetime.utcfromtimestamp(m_n / \
#                            1000).strftime("%H-%M-%S.%f %d.%m.%Y") \
#                        for r in all_time[sender_id] for m in r for m_n in m]) \
#                            + ' - ' + \
#                        max([datetime.utcfromtimestamp(m_n / 1000).strftime(
#                                                    "%H-%M-%S.%f %d.%m.%Y") \
#                        for r in all_time[sender_id] for m in r for m_n in m])
#        err_info += "%4d| %s\n%s\n" % (
#            np.sum(sum_all[sender_id]), sender, ' ' * 6 + '{' + str_time + '}')
#        for event_id, event in enumerate(sorted(errors_dict[sender].keys())):
#            if sum_all[sender_id][event_id] < 3:
#                str_time = ''.join([datetime.utcfromtimestamp(m_n / \
#                                1000).strftime("%H-%M-%S.%f %d.%m.%Y") + '; '
#                            for m in all_time[sender_id][event_id] for m_n in m])
#            else:
#                str_time = min([datetime.utcfromtimestamp(m_n / \
#                                1000).strftime("%H-%M-%S.%f %d.%m.%Y") \
#                        for m in all_time[sender_id][event_id] for m_n in m]) + \
#                        ' - ' + max([datetime.utcfromtimestamp(m_n / \
#                            1000).strftime("%H-%M-%S.%f %d.%m.%Y")
#                        for m in all_time[sender_id][event_id] for m_n in m])
#            err_info += "%8d| %s\n%s\n" % (
#                sum_all[sender_id][event_id], event, ' ' * 10 + '{' + \
#                                                                str_time + '}')
#            if isinstance(errors_dict[sender][event], dict):
#                for message_id, message in enumerate(sorted(errors_dict[sender][
#                                                                event].keys())):
#                    if errors_dict[sender][event][message] < 3:
#                        str_time = ''.join([datetime.utcfromtimestamp(m_n / \
#                                        1000).strftime("%H-%M-%S.%f %d.%m.%Y") + \
#                                    '; ' for m_n in all_time[sender_id][
#                                                        event_id][message_id]])
#                    else:
#                        str_time = min([datetime.utcfromtimestamp(m_n / \
#                                        1000).strftime("%H-%M-%S.%f %d.%m.%Y") \
#                            for m_n in all_time[sender_id][event_id][message_id]]) \
#                                + ' - ' + max([datetime.utcfromtimestamp(m_n / \
#                                        1000).strftime("%H-%M-%S.%f %d.%m.%Y")
#                            for m_n in all_time[sender_id][event_id][message_id]])
#                    err_info += "%12d| %s\n%s\n" % (errors_dict[sender][event][
#                                                    message], message, \
#                                                ' ' * 14 + '{' + str_time + '}')
#        err_info += '\n'
#    return err_info