"""Saving received information about log lines
"""
from datetime import datetime
import os


def print_all_headers(directory, errors, headers, log_format_headers, out):
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


def print_only_dt_message(directory, errors, new_fields, out):
    if errors == []:
        return

    dt_idx = new_fields.index("date_time")
    line_idx = new_fields.index("line_num")
    msg_idx = new_fields.index("message")
    reason_idx = new_fields.index("reason")
    details_idx = new_fields.index("details")
    reason = [err[reason_idx]
              if err[details_idx] == ''
              else err[details_idx] if err[reason_idx] == ''
              else err[reason_idx] + ';' + err[details_idx]
              for err in errors]
    linenum_len = max(len(os.path.join(directory, err[line_idx]))
                      for err in errors)
    full_reason_len = max([len(reason[r]) for r in range(len(reason))])
    out.write("%23s | %*s | %*s | %s\n" % ('Date+Time',
              linenum_len, 'Line', full_reason_len,
              'Reason', 'Message'))
    out.write('-'*(29+linenum_len+full_reason_len+50)+'\n')
    for idx, err in enumerate(errors):
        out.write("%12s %s | %*s | %*s | %s\n" %
                  (datetime.utcfromtimestamp(err[dt_idx]).strftime(
                   "%H:%M:%S,%f")[:-3],
                   datetime.utcfromtimestamp(err[dt_idx]).strftime(
                   "%d-%m-%Y"),
                   linenum_len, os.path.join(directory, err[line_idx]),
                   full_reason_len, reason[idx], err[msg_idx]))
