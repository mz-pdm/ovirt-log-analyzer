import os
import sys
import argparse
from lib.LogAnalyzer import LogAnalyzer

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Parse logfiles and summarize errors into standart output.')
	parser.add_argument('log_directory', \
							metavar='directory', \
							type=str, \
							nargs=1, \
							help='logfiles directory')
	parser.add_argument('log_filenames', \
							metavar='filename', \
							type=str, \
							nargs='+', \
							help='logfiles filenames (without expansion)')
	parser.add_argument("-chart", "--chart_filename", \
							type=str, \
							help="Create html file with chart represented errors statistics")
	parser.add_argument("-print", "--output_descriptor", \
							type=str, \
							help="Where to print output (filename, 'stdout' or 'stderr')")
	parser.add_argument("-o", "--output_file", \
							type=str, \
							help="Directs the output to a name of your choice (with expansion)")
	parser.add_argument("-g", "--graph", \
							type=str, \
							help="Create dot file and pdf with graph of linked errors")
	parser.add_argument("-odir", "--output_directory", \
							type=str, \
							help="Specify directory to save program output")
	args = parser.parse_args()
	
	if args.output_directory is not None:
		if not os.path.isdir(args.output_directory):
			os.mkdir(args.output_directory)
		output_directory = args.output_directory
	else:
		output_directory = ''
	
	if args.output_descriptor is not None:
		if args.output_descriptor == "stdout":
			output_descriptor = sys.stdout
		elif args.output_descriptor == "stderr":
			output_descriptor = sys.stderr
		else:
			sys.stderr = open(os.path.join(output_directory, args.output_descriptor), 'w')
			output_descriptor = sys.stderr
	else:
		output_descriptor = sys.stderr

	if args.output_file is not None:
		output_file = open(os.path.join(output_directory, args.output_file), 'w')
	else:
		output_file = sys.stdout
	
	logs = LogAnalyzer(output_descriptor, args.log_directory[0], args.log_filenames)
	logs.load_data()
	logs.merge_logfiles()
	logs.print_errors(output_file)
	logs.calculate_errors_frequency()
	output_descriptor.write('Creating a chart...\n')
	if args.chart_filename is not None:
		logs.dump_to_json(output_directory, args.chart_filename)
	output_descriptor.write('Linking errors and creating a .dot file with graph...\n')
	if args.graph is not None:
		logs.create_errors_graph(output_directory, args.graph)
	output_descriptor.write('Run "dot -Tpdf ' + os.path.join(output_directory, args.graph) + '.dot -o ' + os.path.join(output_directory, args.graph) + '.pdf" to plot the graph\n')