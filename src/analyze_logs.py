import os
import sys
import argparse
from lib.LogAnalyzer import LogAnalyzer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Parse logfiles and summarize errors into standart output.')
    parser.add_argument('format_templates_filename',
                        metavar='format_filename',
                        type=str,
                        nargs=1,
                        help='filename with formats of log files' + 
                                '(with path and expansion)')    
    parser.add_argument('log_directory',
                        metavar='directory',
                        type=str,
                        nargs=1,
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
                        help="Specify time zones for files "+\
                            "(example: -tz engine -0400 vdsm +0000)"\
                            "Default time zone: UTC")
    #parser.add_argument("-chart", "--chart_filename",
    #                    type=str,
    #                    help="Create html file with chart" + 
    #                            "represented errors statistics")
    parser.add_argument("-print", "--output_descriptor",
                        type=str,
                        help="Where to print output" + 
                                "(filename, 'stdout' or 'stderr')")
    parser.add_argument("-o", "--output_file",
                        type=str,
                        help="Directs the output to a name" + 
                                "of your choice (with expansion)")
    #parser.add_argument("-g", "--graph",
    #                    type=str,
    #                    help="Create dot file and pdf with graph" + 
    #                            "of linked errors")
    parser.add_argument("-odir", "--output_directory",
                        type=str,
                        help="Specify directory to save program output")

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
                print('Wrong filename "%s" in time zone (-tz). '+\
                        'Not listen in log_filenames' % args.tzinfo[file_idx])
                exit()
            tz_info[args.tzinfo[file_idx]] = args.tzinfo[file_idx+1]

    #Output desctiptor
    if args.output_descriptor is not None:
        if args.output_descriptor == "stdout":
            output_descriptor = sys.stdout
        elif args.output_descriptor == "stderr":
            output_descriptor = sys.stderr
        else:
            sys.stderr = open(os.path.join(
                output_directory, args.output_descriptor), 'w')
            output_descriptor = sys.stderr
    else:
        output_descriptor = sys.stderr

    #Output file
    #if args.output_file is not None:
    #    output_file = open(os.path.join(
    #        output_directory, args.output_file), 'w')
    #else:
    #    output_file = sys.stdout
    
    #The algorythm
    logs = LogAnalyzer(output_descriptor, \
                        args.log_directory[0], \
                        args.log_filenames, \
                        tz_info,
                        args.format_templates_filename[0])
    logs.load_data()
    #logs.merge_logfiles()
    #logs.print_errors(output_file)
    #logs.calculate_errors_frequency()
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
