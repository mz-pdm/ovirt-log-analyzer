import os
import re
import sys
import argparse
import pytz
from datetime import datetime
from lib.LogAnalyzer import LogAnalyzer
from lib.detect_running_components import find_all_vm_host

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse logfiles and summarize errors into standart output.')  
    parser.add_argument('-l','--list_vm_host',
                        action = 'store_true',
                        help='Print all VMs and hosts in given time range (or'+\
                        ' without it in all log)')
    parser.add_argument('log_directory',
                        metavar='directory',
                        type=str,
                        help='logfiles directory')
    parser.add_argument('-f','--filenames',
                        type=str,
                        nargs='+',
                        help='logfiles filenames' + 
                                '(without expansion)')
    parser.add_argument( "--default_tzinfo",
                        type=str,
                        nargs='+',
                        help='Specify time zones for all files (will be used)'+\
                            ' if file datetime does not have tz '+\
                            '(example: --default_tzinfo -0400). '\
                            'If not specified - UTC is used')
    parser.add_argument( "--tzinfo",
                        type=str,
                        nargs='+',
                        help='Specify time zones for files (will be used)'+\
                            ' if file datetime does not have tz '+\
                            '(example: --tzinfo engine -0400 vdsm +0100). '\
                            'Default time zone: UTC or set with '+\
                            '--default_tzinfo')
    parser.add_argument("-p" ,"--print",
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
                                '(with path and expansion). Default: ' + \
                                '"format_templates.txt"')  
    parser.add_argument("-t", "--time_range",
                        type=str,
                        nargs='+',
                        help='Specify time range(s) (in UTC) for analysis. ' + \
                        'Type even number of space-separated times (1st for '+\
                        'time range beginning and 2nd for ending) in the ' + \
                        'following format (example) ' + \
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
                        help='Specify event(s) to find information about'+\
                        ' (raw text of event, part of message or a key word),'+\
                        ' use quotes for messages with spaces '+\
                        '(example: --event warning "down with error" failure)')
    parser.add_argument('-w','--warn',
                        action = 'store_true',
                        help='Print parser warnings about different log ' + \
                            'lines format')
    #parser.add_argument("-chart", "--chart_filename",
    #                    type=str,
    #                    help="Create html file with chart" + 
    #                            "represented errors statistics")
    #parser.add_argument("-g", "--graph",
    #                    type=str,
    #                    help="Create dot file and pdf with graph" + 
    #                            "of linked errors")

    args = parser.parse_args()

    #Logfilenames
    if args.filenames is not None:
        files = sorted(args.filenames)
    else:
        files = os.listdir(args.log_directory)
        files = [f for f in files if '.log' in f]
    #Output directory
    if args.output_dir is not None:
        if not os.path.isdir(args.output_dir):
            os.mkdir(args.output_dir)
        output_directory = args.output_dir
    else:
        output_directory = ''
    
    # Time zones
    tz_info = {}
    if args.default_tzinfo is not None:
        default_tz = args.default_tzinfo
    else:
        default_tz = '+0000'

    for filename in files:
        tz_info[filename] = default_tz
    if args.tzinfo is not None:
        if len(args.tzinfo)%2 != 0:
            print('Argparser: Wrong number of arguments for time zone (-tz). '+\
                    'Must be even')
            exit()
        for file_idx in range(0,len(args.tzinfo)-1,2):
            if args.tzinfo[file_idx] not in args.files:
                print('Argparser: Wrong filename "%s" in time zone '+ \
                    '(was not listed in log_filenames)' % args.tzinfo[file_idx])
                exit()
            elif re.fullmatch(r"^[\+\-][\d]{2}00$", \
                            args.tzinfo[file_idx+1]) \
                        is None:
                print('Argparser: Wrong time zone format %s for file %s'% \
                        (args.tzinfo[file_idx+1], args.tzinfo[file_idx]))
                exit()
            tz_info[args.tzinfo[file_idx]] = args.tzinfo[file_idx+1]

    # Time ranges
    time_range_info = []
    if args.time_range is not None:
        if len(args.time_range)%2 != 0:
            print('Argparser: Wrong number of arguments for time range ' + \
                    '(-time). Must be even')
            exit()
        for tr_idx in range(0,len(args.time_range)-1,2):
            try:
                date_time_1 = datetime.strptime(args.time_range[tr_idx], \
                                                "%Y-%m-%dT%H:%M:%S,%f")
                date_time_1 = date_time_1.replace(tzinfo=pytz.utc)
                date_time_1 = date_time_1.timestamp()
            except ValueError:
                print('Argparser: Wrong datetime format: %s' \
                        % args.time_range[tr_idx])
                exit()
            try:
                date_time_2 = datetime.strptime(args.time_range[tr_idx+1], \
                                                "%Y-%m-%dT%H:%M:%S,%f")
                date_time_2 = date_time_2.replace(tzinfo=pytz.utc)
                date_time_2 = date_time_2.timestamp()
            except ValueError:
                print('Argparser: Wrong datetime format: %s' \
                        % args.time_range[tr_idx+1])
                exit()
            if date_time_2 < date_time_1:
                print("Argparser: Provided date time range doesn't overlaps: "+\
                        "%s %s" % (args.time_range[tr_idx], \
                                    args.time_range[tr_idx+1]))
                exit()
            time_range_info += [[date_time_1, date_time_2]]

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

    #Output desctiptor
    if args.print is not None:
        if args.print == "stdout":
            output_descriptor = sys.stdout
        elif args.print == "stderr":
            output_descriptor = sys.stderr
        else:
            sys.stderr = open(os.path.join(output_directory, \
                        args.print), 'w')
            output_descriptor = sys.stderr
    else:
        output_descriptor = sys.stderr

    #Output file
    if args.out is not None:
        output_file = open(os.path.join(
            output_directory, args.out), 'w')
    else:
        output_file = sys.stdout
    
    #Format templates
    if args.format_file is not None:
        format_file = args.format_file
    else:
        format_file = os.path.join("format_templates.txt")
    
    if args.list_vm_host:
        vm_ids, host_ids = find_all_vm_host(output_descriptor,
                            args.log_directory,
                            files,
                            tz_info,
                            time_range_info)
        output_descriptor.write('------- List of VMs -------\n')
        for vm in sorted(vm_ids.keys()):
            output_descriptor.write('ID: %s\n' % vm)
            output_descriptor.write('name: %s\n' % vm_ids[vm])
            output_descriptor.write('\n')
        output_descriptor.write('------- List of Hosts -------\n')
        for host in sorted(host_ids.keys()):
            output_descriptor.write('ID: %s\n' % host)
            output_descriptor.write('name: %s\n' % host_ids[host])
            output_descriptor.write('\n')
        exit()
    logs = LogAnalyzer(output_descriptor,
                        args.log_directory,
                        files,
                        tz_info,
                        time_range_info,
                        vm_info,
                        event_info,
                        host_info,
                        format_file)
    logs.load_data(args.warn)
    #now just all relevant lines
    messages = logs.find_rare_errors()
    logs.print_errors(messages, output_file)

    #these options are on the way
    #if args.chart_filename is not None:
    #    output_descriptor.write('Creating a chart...\n')
    #    logs.dump_to_json(output_directory, args.chart_filename)
    #if args.graph is not None:
    #    output_descriptor.write(
    #    'Linking errors and creating a .dot file with graph...\n')
    #    logs.create_errors_graph(output_directory, args.graph)
    #    output_descriptor.write('Run "dot -Tpdf ' + 
    #                        os.path.join(output_directory, args.graph) + 
    #                        '.dot -o ' + 
    #                        os.path.join(output_directory, args.graph) + 
    #                        '.pdf" to plot the graph\n')
