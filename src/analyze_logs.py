import os
import re
import sys
import argparse
import pytz
from datetime import datetime
from lib.LogAnalyzer import LogAnalyzer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse logfiles and summarize errors into standart output.')  
    parser.add_argument('log_directory',
                        metavar='directory',
                        type=str,
                        help='logfiles directory')
    parser.add_argument('log_filenames',
                        metavar='filename',
                        type=str,
                        nargs='+',
                        help='logfiles filenames' + 
                                '(without expansion)')
    parser.add_argument("-tz", "--tzinfo",
                        type=str,
                        nargs='+',
                        help='Specify time zones for files '+\
                            '(example: -tz engine -0400 vdsm +0000)'\
                            'Default time zone: UTC')
    #parser.add_argument("-chart", "--chart_filename",
    #                    type=str,
    #                    help="Create html file with chart" + 
    #                            "represented errors statistics")
    parser.add_argument("-print", "--output_descriptor",
                        type=str,
                        help='Where to print output' + 
                                '(filename, "stdout" or "stderr")')
    parser.add_argument("-o", "--output_file",
                        type=str,
                        help='Directs the output to a name' + 
                                'of your choice (with expansion)')
    #parser.add_argument("-g", "--graph",
    #                    type=str,
    #                    help="Create dot file and pdf with graph" + 
    #                            "of linked errors")
    parser.add_argument("-odir", "--output_directory",
                        type=str,
                        help='Specify directory to save program output')
    parser.add_argument('-format', '--format_templates_filename',
                        type=str,
                        help='filename with formats of log files' + 
                                '(with path and expansion)')  
    parser.add_argument("-time", "--time_range",
                        type=str,
                        nargs='+',
                        help='Specify time range(s) (in UTC) for analysis. ' + \
                        'Type even number of space-separated times (1st for '+\
                        'time range beginning and 2nd for ending) in the ' + \
                        'following format (example)' + \
                        '2000-01-31T21:10:00,123 2000-01-31T22:10:00,123')
    parser.add_argument("-vm",
                        type=str,
                        nargs='+',
                        help='Specify VM id(s) to find information about them')
    parser.add_argument("-host",
                        type=str,
                        nargs='+',
                        help='Specify host id(s) to find information about them')
    parser.add_argument("-event",
                        type=str,
                        nargs='+',
                        help='Specify event(s) to find information about them'+\
                        ' (raw text of event, part of message or key word), '+\
                        'use quotes for messages with spaces '+\
                        '(e.g. -event warning "down with error" failure)')

    args = parser.parse_args()

    #Output directory
    if args.output_directory is not None:
        if not os.path.isdir(args.output_directory):
            os.mkdir(args.output_directory)
        output_directory = args.output_directory
    else:
        output_directory = ''
    
    # Time zones
    tz_info = {}
    for filename in args.log_filenames:
        tz_info[filename] = '+0000'
    if args.tzinfo is not None:
        if len(args.tzinfo)%2 != 0:
            print('Wrong number of arguments for time zone (-tz). Must be even')
            exit()
        for file_idx in range(0,len(args.tzinfo)-1,2):
            if args.tzinfo[file_idx] not in args.log_filenames:
                print('Wrong filename "%s" in time zone (was not listed in '+\
                    'log_filenames)' % args.tzinfo[file_idx])
                exit()
            elif re.fullmatch(r"^[\+\-][\d]{2}00$", \
                            args.tzinfo[file_idx+1]) \
                        is None:
                print('Wrong time zone format %s for file %s'% \
                        (args.tzinfo[file_idx+1], args.tzinfo[file_idx]))
                exit()
            tz_info[args.tzinfo[file_idx]] = args.tzinfo[file_idx+1]

    # Time ranges
    time_range_info = []
    if args.time_range is not None:
        if len(args.time_range)%2 != 0:
            print('Wrong number of arguments for time range (-time). '+\
                    'Must be even')
            exit()
        for tr_idx in range(0,len(args.time_range)-1,2):
            print(args.time_range[tr_idx])
            try:
                date_time_1 = datetime.strptime(args.time_range[tr_idx], \
                                                "%Y-%m-%dT%H:%M:%S,%f")
                date_time_1 = date_time_1.replace(tzinfo=pytz.utc)
                date_time_1 = date_time_1.timestamp()
            except ValueError:
                print('Wrong datetime format: %s'% args.time_range[tr_idx])
                exit()
            try:
                date_time_2 = datetime.strptime(args.time_range[tr_idx+1], \
                                                "%Y-%m-%dT%H:%M:%S,%f")
                date_time_2 = date_time_2.replace(tzinfo=pytz.utc)
                date_time_2 = date_time_2.timestamp()
            except ValueError:
                print('Wrong datetime format: %s'% args.time_range[tr_idx+1])
                exit()
            if date_time_2 < date_time_1:
                print("Provided date time range doesn't overlaps: "+\
                        "%s %s" % (args.time_range[tr_idx], \
                                    args.time_range[tr_idx+1]))
                exit()
            time_range_info += [date_time_1, date_time_2]

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
    if args.output_descriptor is not None:
        if args.output_descriptor == "stdout":
            output_descriptor = sys.stdout
        elif args.output_descriptor == "stderr":
            output_descriptor = sys.stderr
        else:
            sys.stderr = open(os.path.join(output_directory, \
                        args.output_descriptor), 'w')
            output_descriptor = sys.stderr
    else:
        output_descriptor = sys.stderr

    #Output file
    if args.output_file is not None:
        output_file = open(os.path.join(
            output_directory, args.output_file), 'w')
    else:
        output_file = sys.stdout
    
    #Format templates
    if args.format_templates_filename is not None:
        format_file = args.format_templates_filename
    else:
        format_file = os.path.join("format_templates.txt")
    
    #The algorythm
    logs = LogAnalyzer(output_descriptor,
                        args.log_directory,
                        args.log_filenames,
                        tz_info,
                        time_range_info,
                        vm_info,
                        event_info,
                        host_info,
                        format_file)
    logs.load_data()
    #logs.find_rare_errors()
    #logs.print_errors(output_file)
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
