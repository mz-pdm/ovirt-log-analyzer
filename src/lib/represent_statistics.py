"""Saving received information about log lines
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
    if errors == []:
        return

    dt_idx = new_fields.index("date_time")
    line_idx = new_fields.index("line_num")
    msg_idx = new_fields.index("message")
    reason_idx = new_fields.index("reason")
    details_idx = new_fields.index("details")

    reason_len = max([len(r[reason_idx]) for r in errors])
    linenum_len = max(len(err[line_idx]) for err in errors)
    details_len = max([len(r[details_idx]) for r in errors])
    out.write("%23s | %*s | %*s | %*s | %s\n" % ('Date+Time',
              linenum_len, 'Line', reason_len, 'Reason', details_len,
              'Details', 'Message'))
    out.write('-'*(29+linenum_len+reason_len+details_len+50)+'\n')
    for idx, err in enumerate(errors):
        out.write("%12s %s | %*s | %*s | %*s | %s\n" %
                  (datetime.utcfromtimestamp(err[dt_idx]).strftime(
                   "%H:%M:%S,%f")[:-3],
                   datetime.utcfromtimestamp(err[dt_idx]).strftime(
                   "%d-%m-%Y"),
                   linenum_len, err[line_idx], reason_len, err[reason_idx],
                   details_len, err[details_idx], err[msg_idx]))
