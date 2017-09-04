import os
import sys
import argparse
import re
import pytz
from datetime import datetime
from lib.LogAnalyzer import LogAnalyzer
from lib.util import open_log_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse logfiles and summarize important messages ' +
                    ' into standard output or file')
    parser.add_argument('-l', '--list_vm_host',
                        action='store_true',
                        help='Print all VMs and hosts in given time range ' +
                        '(or without it in all log)')
    parser.add_argument('log_directory',
                        metavar='directory',
                        type=str,
                        help='logfiles directory')
    parser.add_argument('-f', '--filenames',
                        type=str,
                        nargs='+',
                        help='logfiles filenames' +
                        '(with expansion); use . to find common log files')
    parser.add_argument("--default_tzinfo",
                        type=str,
                        nargs='+',
                        help='Specify time zones for all files ' +
                        '(will be used) if file datetime does not have ' +
                        'time zone ' +
                        '(example: --default_tzinfo -0400). ' +
                        'If not specified - UTC is used')
    parser.add_argument("--tzinfo",
                        type=str,
                        nargs='+',
                        help='Specify time zones for files (will be used)' +
                        ' if file datetime does not have tz ' +
                        '(example: --tzinfo engine -0400 vdsm +0100). ' +
                        'Default time zone: UTC or set with ' +
                        '--default_tzinfo')
    parser.add_argument("-p", "--print",
                        type=str,
                        help='Where to print the output ' +
                        '(filename, "stdout" or "stderr")')
    parser.add_argument("-o", "--out",
                        type=str,
                        help='Directs the output to the file')
    parser.add_argument("-d", "--output_dir",
                        type=str,
                        help='Specify directory to save program output')
    parser.add_argument('--format_file',
                        type=str,
                        help='Filename with formats of log files ' +
                        '(with path and expansion). Default: ' +
                        '"format_templates.txt"')
    parser.add_argument("-t", "--time_range",
                        type=str,
                        nargs='+',
                        help='Specify time range(s) (in UTC) for analysis. ' +
                        'Type even number of space-separated times (1st for ' +
                        'time range beginning and 2nd for ending) in the ' +
                        'following format (example) ' +
                        '2000-01-31T21:10:00,123 2000-01-31T22:10:00,123')
    parser.add_argument("--vm",
                        type=str,
                        nargs='+',
                        help='Specify VM names and uuid to find ' +
                             'information about (example: --vm VM1 ' +
                             '0000-0001 VM2 VM3 0000-0003)')
    parser.add_argument("--host",
                        type=str,
                        nargs='+',
                        help='Specify host id(s) to find information about')
    parser.add_argument("--event",
                        type=str,
                        nargs='+',
                        help='Specify event(s) to find information about' +
                        ' (raw text of event, part of message or a key ' +
                        ' word), use quotes for messages with spaces ' +
                        '(example: --event warning "down with error" failure)')
    parser.add_argument('-w', '--warn',
                        action='store_true',
                        help='Print parser warnings about different log ' +
                        'lines format')
    parser.add_argument('--progressbar',
                        action='store_true',
                        help='Show full-screen progress bar for parsing ' +
                             'process')
    parser.add_argument('--additive',
                        action='store_true',
                        help='Search for messages that contain user-defined' +
                             'VMs OR hosts')
    parser.add_argument('--criterias',
                        type=str,
                        nargs='+',
                        help='Criterias of adding a message to the output.' +
                             ' Available: "VM id", "Subtasks", ' +
                             '"Error or warning", "Differ by VM ID", ' +
                             '"Exclude frequent messages", "Coverage", ' +
                             '"Increased errors", "Long operations". ' +
                             'Default is all')
    # parser.add_argument("-chart", "--chart_filename",
    #                    type=str,
    #                    help="Create html file with chart" +
    #                    "represented errors statistics")
    args = parser.parse_args()

    # Logfilenames
    def log_file_p(file_name):
        base_file_name = os.path.basename(file_name)
        return ('.log' in base_file_name and
                not base_file_name.endswith('.json'))
    if args.filenames is None or args.filenames == ['.']:
        files = []
        for dirpath, dirnames, filenames in os.walk(args.log_directory):
            for f in filenames:
                if (log_file_p(f) and
                    (args.filenames is None or
                     dirpath == 'qemu' or
                     f.startswith('engine') or
                     'libvirt' in f or
                     'vdsm' in f)):
                    files.append(os.path.join(dirpath, f))
    elif args.filenames is not None:
        files = sorted(args.filenames)
    # Output directory
    if args.output_dir is not None:
        if not os.path.isdir(args.output_dir):
            os.mkdir(args.output_dir)
        output_directory = args.output_dir
    else:
        output_directory = ''
    # Time zones
    tz_info = {}
    if args.default_tzinfo is not None:
        default_tz = args.default_tzinfo[0]
    else:
        default_tz = '+0000'
    for filename in files:
        tz_info[filename] = default_tz
    if args.tzinfo is not None:
        if len(args.tzinfo) % 2 != 0:
            print('Argparser: Wrong number of arguments for time zone ' +
                  '(-tz). Must be even')
            exit()
        for file_idx in range(0, len(args.tzinfo)-1, 2):
            if args.tzinfo[file_idx] not in files:
                print(('Argparser: Wrong filename %s in time zone ' +
                      '(was not listed in log_filenames)') %
                      args.tzinfo[file_idx])
                exit()
            elif re.fullmatch(r"^[\+\-][\d]{2}00$",
                              args.tzinfo[file_idx+1]) is None:
                print('Argparser: Wrong time zone format %s for file %s' %
                      (args.tzinfo[file_idx+1], args.tzinfo[file_idx]))
                exit()
            tz_info[args.tzinfo[file_idx]] = args.tzinfo[file_idx+1]
    # Time ranges
    time_range_info = []
    if args.time_range is not None:
        if len(args.time_range) % 2 != 0:
            print('Argparser: Wrong number of arguments for time range ' +
                  '(-time). Must be even')
            exit()
        for tr_idx in range(0, len(args.time_range)-1, 2):
            try:
                date_time_1 = datetime.strptime(args.time_range[tr_idx],
                                                "%Y-%m-%dT%H:%M:%S,%f")
                date_time_1 = date_time_1.replace(tzinfo=pytz.utc)
                date_time_1 = date_time_1.timestamp()
            except ValueError:
                print('Argparser: Wrong datetime format: %s' %
                      args.time_range[tr_idx])
                exit()
            try:
                date_time_2 = datetime.strptime(args.time_range[tr_idx+1],
                                                "%Y-%m-%dT%H:%M:%S,%f")
                date_time_2 = date_time_2.replace(tzinfo=pytz.utc)
                date_time_2 = date_time_2.timestamp()
            except ValueError:
                print('Argparser: Wrong datetime format: %s' %
                      args.time_range[tr_idx+1])
                exit()
            if date_time_2 < date_time_1:
                print(("Argparser: Provided date time range doesn't " +
                      "overlap: %s %s") % (args.time_range[tr_idx],
                                           args.time_range[tr_idx+1]))
                exit()
            time_range_info += [[date_time_1, date_time_2]]
        time_range_info = sorted(time_range_info, key=lambda k: k[0])
    # VMs
    if args.vm is not None:
        vm_info = args.vm
    else:
        vm_info = []
    # Hosts
    if args.host is not None:
        host_info = args.host
    else:
        host_info = []
    # Events
    if args.event is not None:
        event_info = args.event
    else:
        event_info = []
    # Output desctiptor
    if args.print is not None:
        if args.print == "stdout":
            output_descriptor = sys.stdout
        elif args.print == "stderr":
            output_descriptor = sys.stderr
        else:
            sys.stderr = open(os.path.join(output_directory, args.print), 'w')
            output_descriptor = sys.stderr
    else:
        output_descriptor = sys.stderr
    # Format templates
    if args.format_file is not None:
        format_file = args.format_file
    else:
        format_file = os.path.join("format_templates.txt")
    if args.criterias is not None:
        criterias = args.criterias
    else:
        criterias = ['All']
    # Start algo
    logs = LogAnalyzer(output_descriptor,
                       args.log_directory,
                       files,
                       tz_info,
                       criterias,
                       time_range_info,
                       vm_info,
                       event_info,
                       host_info,
                       format_file,
                       args.additive,
                       output_directory)
    output_descriptor.write('Reading file\'s time range...\n')
    logs.read_time_ranges()
    output_descriptor.write('Searching for running VMs and hosts...\n')
    logs.find_vms_and_hosts()
    if args.list_vm_host:
        output_descriptor.write('------- List of files\' time ranges (UTC) ' +
                                '-------\n')
        for log in sorted(logs.total_time_ranges.keys()):
            output_descriptor.write(('%s: %s %s\n') % (log,
                                    datetime.utcfromtimestamp(
                                      logs.total_time_ranges[log][0]).strftime(
                                        "%Y-%m-%dT%H:%M:%S,%f")[:-3],
                                    datetime.utcfromtimestamp(
                                      logs.total_time_ranges[log][1]).strftime(
                                        "%Y-%m-%dT%H:%M:%S,%f")[:-3]))
        if (logs.found_logs != []):
            output_descriptor.write('____________________\n')
            max_time = max([t for l in logs.total_time_ranges.keys()
                            for t in logs.total_time_ranges[l]])
            min_time = min([t for l in logs.total_time_ranges.keys()
                            for t in logs.total_time_ranges[l]])
            output_descriptor.write(('Total: %s %s\n') %
                                    (datetime.utcfromtimestamp(
                                        min_time).strftime(
                                            "%Y-%m-%dT%H:%M:%S,%f")[:-3],
                                     datetime.utcfromtimestamp(
                                        max_time).strftime(
                                            "%Y-%m-%dT%H:%M:%S,%f")[:-3]))
            output_descriptor.write('\n')
        output_descriptor.write('------- List of VMs -------\n')
        for vm in sorted(logs.all_vms.keys()):
            output_descriptor.write('Name: %s\n' % vm)
            output_descriptor.write('IDs:')
            for vmid in sorted(list(logs.all_vms[vm]['id'])):
                output_descriptor.write(' %s' % vmid)
            output_descriptor.write('\n')
            output_descriptor.write('Found on hosts:')
            for hid in sorted(list(logs.all_vms[vm]['hostids'])):
                output_descriptor.write(' %s' % hid)
            output_descriptor.write('\n')
            output_descriptor.write('\n')
        if logs.not_running_vms != []:
            output_descriptor.write('------------------------\n')
            output_descriptor.write('Created but not running VMs:')
            for vname in logs.not_running_vms:
                output_descriptor.write(' %s' % vname)
            output_descriptor.write('\n')
        if logs.not_found_vmnames != []:
            output_descriptor.write('VMs with not found names:')
            for vid in logs.not_found_vmnames:
                output_descriptor.write(' %s' % vid)
            output_descriptor.write('\n')
            output_descriptor.write('------------------------\n')
        output_descriptor.write('------- List of Hosts -------\n')
        for host in sorted(logs.all_hosts.keys()):
            output_descriptor.write('Name: %s\n' % host)
            output_descriptor.write('IDs:')
            for hostid in sorted(list(logs.all_hosts[host]['id'])):
                output_descriptor.write(' %s' % hostid)
            output_descriptor.write('\n')
            output_descriptor.write('Running VMs:')
            for vid in sorted(list(logs.all_hosts[host]['vmids'])):
                output_descriptor.write(' %s' % vid)
            output_descriptor.write('\n')
            output_descriptor.write('\n')
        output_descriptor.write('------------------------\n')
        if logs.not_found_hostnames != []:
            output_descriptor.write('Hosts with not found names:')
            for hid in logs.not_found_hostnames:
                output_descriptor.write(' %s' % hid)
            output_descriptor.write('\n')
        exit()
    output_descriptor.write('Searching for VM tasks...\n')
    logs.find_vm_tasks()
    output_descriptor.write('Loading data...\n')
    logs.load_data(args.warn, args.progressbar)
    output_descriptor.write('Analyzing the messages...\n')
    logs.merge_all_messages()
    messages, new_fields = logs.find_important_events()
    output_descriptor.write('Printing messages...\n')
    # Output file
    if args.out is not None:
        output_file = open(os.path.join(output_directory, args.out), 'w')
    else:
        output_file = sys.stdout
    logs.print_errors(messages, new_fields, output_file)
