import numpy as np
import os
import argparse
import re
import dateutil.parser
import pytz
from pytz import timezone
from datetime import datetime, date, time

def DatetimeParser(date_time):
	error_date = date_time.partition(" ")[0]
	error_rest = date_time.partition(" ")[2]
	error_time = error_rest.partition(",")[0]
	
	error_tz = error_rest.partition(",")[2][3:]
	try:
		#print(date_time.partition(",")[0] + " " + date_time.partition(",")[2][3:])
		dt = datetime.strptime(date_time.partition(",")[0] + date_time.partition(",")[2][3:], "%Y-%m-%d %H:%M:%S %Z").replace(tzinfo=pytz.utc)
	except ValueError:
		pass
		print(date_time, ': Unknown format')
	return (error_date, error_time)

def MessageParser(message):
	objects = ['SQL', 'SSH', 'host']
	msg_obj = [word for word in message.split(' ') if word in objects]
	msg_text = []
	template = re.compile("[\]\)\}\>].{10,}?[\[\(\{\<\n]")
	#message = re.sub('\n', '', message)
	for text_found in re.findall(template, message):
		text_found = re.sub('[\)\]\>\}\{\<\[\(]', '', text_found)
		text_found = re.sub(' ', '_', text_found)
		if text_found.partition("Message")[2] != '':
			text_found = text_found.partition("Message")[2]
		text_found = re.sub('\n', '', text_found)
		msg_text += [text_found]
	return msg_text, msg_obj

def SummarizeErrors(error_text):
	errors_dict = {}
	for err in error_text:
		if not(err[0] in errors_dict.keys()):
			errors_dict[err[0]] = {}
		for ms_idx in range(1, len(err)):
			if err[ms_idx] in errors_dict[err[0]].keys():
				errors_dict[err[0]][err[ms_idx]] += 1
			else:
				errors_dict[err[0]][err[ms_idx]] = 1
	return errors_dict

def PrintStatistics(logfile_name, errors_dict, errors_num):
	err_info = '________________________________________\n\n'
	err_info += "Summarize statistics for " + logfile_name + '\n'
	err_info += "Number of error messages: " + str(errors_num) + '\n'
	err_info += '________________________________________\n'
	for i in errors_dict.keys():
		sum_errors = np.sum([errors_dict[i][err_k] for err_k in errors_dict[i].keys()])
		err_info += "%4d| %s\n" % (sum_errors if sum_errors > 0 else 1, i)
		for j in errors_dict[i].keys():
			err_info += "%8d| %s\n" % (errors_dict[i][j], j)
		err_info += '----------------------------------------\n'
	#print(err_info)
	return err_info

def main():
	parser = argparse.ArgumentParser(description='Parse logfiles and summarize errors into standart output.')
	parser.add_argument('log_directory', metavar='directory', type=str, nargs=1, help='logfiles directory')
	parser.add_argument('log_filenames', metavar='filename', type=str, nargs='+', help='logfiles filenames (with expansion)')
	parser.add_argument("-o", "--output", help="Directs the output to a name of your choice")
	args = parser.parse_args()
	
	output_text = ''
	for log in args.log_filenames:
		#error_date = []
		#error_time = []
		error_msg_text = []
		error_msg_obj = []
		print('Analysing', args.log_directory[0] + '/' + log, '...')
		if not os.path.isfile(args.log_directory[0] + '/' + log):
			print("File not found: %s\n" % log)
			continue

		with open(args.log_directory[0] + '/' + log) as f:
			for line in f:
				if 'ERROR' in line:
					#error_datetime = line.partition(" ERROR")[0]
					#error_date_and_time = DatetimeParser(error_datetime)
					#error_date += [error_date_and_time[0]]
					#error_time += [error_date_and_time[1]]
					error_msg_info = MessageParser(line.partition(" ERROR ")[2])
					error_msg_text += [error_msg_info[0]]
					error_msg_obj += [error_msg_info[1]]
	
		summarize_errors = SummarizeErrors(error_msg_text)	
		output_text += PrintStatistics(log, summarize_errors, len(error_msg_text))
	
	if args.output is not None:
		output_file = open(args.output, 'w')
 		#output_file.write("%s:\n" % log)
		output_file.write(output_text)
		output_file.close()
	else:
		print(output_text)	
main()