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
	parser.add_argument("-err", "--out_descr", \
							help="Where to print output (filename, 'stdout' or 'stderr')")
	parser.add_argument("-o", "--output", \
							help="Directs the output to a name of your choice (with expansion)")
	parser.add_argument("-js", "--json", \
							action='store_true', \
							help="Create js file with errors statistics in json")
	parser.add_argument("-g", "--dot", \
							help="Create dot file and pdf with graph of linked errors")
	parser.add_argument("-odir", "--out_dir", \
							help="Specify directory to save program output")
	args = parser.parse_args()


	if args.out_descr is not None:
		if not args.out_descr in ("stdout", "stderr"):
			out_descr = open(args.out_descr, 'w')
		else:
			out_descr = sys.stderr
	else:
		out_descr = sys.stderr
	
	if args.out_dir is not None:
		if not os.path.isdir(args.out_dir):
			os.mkdir(args.out_dir)
		out_dir = args.out_dir
	else:
		out_dir = ''
	
	if args.output is not None:
		output_file = open(os.path.join(out_dir, args.output), 'w')
	else:
		output_file = sys.stdout
	
	logs = LogAnalyzer(out_descr, args.log_directory[0], args.log_filenames)
	logs.load_data()
	logs.merge_logfiles()
	logs.print_errors(output_file)
	logs.calculate_errors_frequency()

	if args.json:
		logs.dump_to_json(out_dir)
	if args.dot is not None:
		logs.create_errors_graph(out_dir, args.dot)