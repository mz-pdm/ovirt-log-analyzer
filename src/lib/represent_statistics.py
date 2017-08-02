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
from datetime import datetime


def print_all_headers(errors, headers, log_format_headers, out):
    for err in errors:
        format_name = err[1].split(':')[0]
        for h in headers:
            if h in log_format_headers[format_name]:
                out.write("%s %s\n" %
                          (h,
                           err[log_format_headers[
                               format_name].index(h)]))
            else:
                out.write("%s %s\n" % (h, ''))
        out.write("\n")


def print_only_dt_message(errors, new_fields, out):
    dt_idx = new_fields.index("date_time")
    line_idx = new_fields.index("line_num")
    msg_idx = new_fields.index("message")
    reason_idx = new_fields.index("reason")
    reason_len = max([len(r[reason_idx]) for r in errors])
    max_len = max(len(err[line_idx]) for err in errors)
    out.write("%23s | %*s | %*s | %s\n" % ('Date+Time', max_len, 'Line',
              reason_len, 'Reason', 'Message'))
    out.write('-'*(29+max_len+reason_len+50)+'\n')
    for idx, err in enumerate(errors):
        #if 'Failed to migrate' in err[msg_idx]:
        #    print(err[msg_idx])
        out.write("%12s %s | %*s | %*s | %s\n" %
                  (datetime.utcfromtimestamp(err[dt_idx]).strftime(
                   "%H:%M:%S,%f")[:-3],
                   datetime.utcfromtimestamp(err[dt_idx]).strftime(
                   "%d-%m-%Y"),
                   max_len, err[line_idx], reason_len, err[reason_idx],
                   err[msg_idx]))
