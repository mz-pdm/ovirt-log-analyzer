import os
import sys
import argparse
import re
import pytz
from datetime import datetime
from lib.LogAnalyzer import LogAnalyzer
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse logfiles and summarize errors into standart ' +
        'output.')
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
                        '(without expansion)')
    parser.add_argument("--default_tzinfo",
                        type=str,
                        nargs='+',
                        help='Specify time zones for all files ' +
                        '(will be used) if file datetime does not have ' +
                        'time zome ' +
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
                        help='Specify VM id(s) to find information about')
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
    # parser.add_argument("-chart", "--chart_filename",
    #                    type=str,
    #                    help="Create html file with chart" +
    #                    "represented errors statistics")
    args = parser.parse_args()

    # Logfilenames
    if args.filenames is not None:
        files = sorted(args.filenames)
    else:
        files = os.listdir(args.log_directory)
        files = [f for f in files if '.log' in f or '.xz' in f]
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

    # VMs
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

    # Output file
    if args.out is not None:
        output_file = open(os.path.join(output_directory, args.out), 'w')
    else:
        output_file = sys.stdout

    # Format templates
    if args.format_file is not None:
        format_file = args.format_file
    else:
        format_file = os.path.join("format_templates.txt")

    logs = LogAnalyzer(output_descriptor,
                       args.log_directory,
                       files,
                       tz_info,
                       time_range_info,
                       vm_info,
                       event_info,
                       host_info,
                       format_file)
    output_descriptor.write('Searching for running VMs and hosts...\n')
    logs.find_vms_and_hosts()
    output_descriptor.write('Searching for VM tasks...\n')
    logs.find_vm_tasks()
    if args.list_vm_host:
        output_descriptor.write('------- List of VMs -------\n')
        for vm in sorted(logs.all_vms.keys()):
            output_descriptor.write('name: %s\n' % vm)
            for i in range(len(logs.all_vms[vm])):
                output_descriptor.write('ID: %s\n' % logs.all_vms[vm][i])
            output_descriptor.write('\n')
        output_descriptor.write('------- List of Hosts -------\n')
        for host in sorted(logs.all_hosts.keys()):
            output_descriptor.write('name: %s\n' % host)
            for i in range(len(logs.all_hosts[host])):
                output_descriptor.write('ID: %s\n' % logs.all_hosts[host][i])
            output_descriptor.write('\n')
        exit()
    output_descriptor.write('Loading data...\n')
    logs.load_data(args.warn, args.progressbar)
    output_descriptor.write('Analyzing the messages...\n')
    messages, new_fields = logs.find_important_events()
    output_descriptor.write('Printing messages...\n')
    logs.print_errors(messages, new_fields, output_file)

    # these options are on the way
    # if args.chart_filename is not None:
    #    output_descriptor.write('Creating a chart...\n')
    #    logs.dump_to_json(output_directory, args.chart_filename)
