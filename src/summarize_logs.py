import numpy as np
import os
import argparse
import re
import dateutil.parser
import pytz
import json
import time
from pytz import timezone
from datetime import datetime, date

from pytz import all_timezones

from optparse import OptionParser
import inspect
class ErrorLog:
	#date_time = 0
	#thread = ''
	#who_send = ''
	#what_happened = ''
	#message = ''
	def __init__(self, line):
		self.raw_text = line
	def ParseDateTime(self):
		dt = self.raw_text.partition(" ERROR ")[0]
		if dt.partition(',')[2][3:] == 'Z':
			dt = dt.replace('Z','+0000')
		try:
			self.date_time = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S,%f%z").replace(tzinfo=pytz.utc)
			#print('Time: ', self.date_time)
			return True
		except ValueError:
			print('Unknown format:', dt)
			return False
	def ParseSender(self):
		template = re.compile(r"\[(.*?)\]")
		t = re.search(template, self.raw_text.partition(" ERROR ")[2])
		if t is not None:
			self.who_send = t.group(1)
			#print('Sender: ', t.group(1))
			return True
		else:
			print('Sender was not found: ', self.raw_text.partition(" ERROR ")[2])
			return False
	def ParseThread(self):
		template = re.compile(r"\((.*?)\)")
		t = re.search(template, self.raw_text.partition(" ERROR ")[2])
		if t:
			self.thread = t.group(1)
			#print('Thread: ', t.group(1))
			return True
		else:
			print('Thread was not found: ', self.raw_text.partition(" ERROR ")[2])
			return False
	def ParseMessage(self):
		template = re.compile(r"Message: (.*)")
		t = re.search(template, self.raw_text.partition(" ERROR ")[2])
		if t is not None and len(t.group(1)) > 5:
			t_mes = re.split(r":", t.group(1))
			if len(t_mes) > 1:
				self.what_happened = re.sub(r"\..*", '', t_mes[0])
				self.message = t_mes[1:]
			else:
				t_mes_underlying = re.split(r"message", t_mes[0])
				if len(t_mes_underlying) > 1:
					self.what_happened = t_mes_underlying[0]
					self.message = t_mes_underlying[1:]
				else:
					self.what_happened = 'Unknown'
					self.message = t_mes
			#print('What_happened = ', self.what_happened)
			#print('MESSAGE = ', self.message)
			return True
		else:
			t_mes = re.split(r"message", self.raw_text.partition(" ERROR ")[2])
			if len(t_mes) == 2:
				self.message = [re.sub('[:\{\}\'\n]', '', t_mes[1])]
				self.what_happened = 'Unknown'
				#print('What_happened = ', self.what_happened)
				#print('Message = ', self.message)
				return True
			template = re.compile(r"(\[.*?\]|\(.*?\)|\{.*?\}|\<.*?\>) *")
			t = re.sub(template, '', self.raw_text.partition(" ERROR ")[2])
			t = re.sub('\n', '', t)
			if t is not None and len(t) > 5:
				t_mes = re.split(r":", t)
				if len(t_mes) > 1:
					self.what_happened = t_mes[0]
					self.message = t_mes[1:]
				else:
					t_mes_underlying = re.split(r"message", t_mes[0])
					if len(t_mes_underlying) > 1:
						self.what_happened = t_mes_underlying[0]
						self.message = t_mes_underlying[1:]
					else:
						self.what_happened = 'Unknown'
						self.message = t_mes
				#print('What_happened = ', self.what_happened)
				#print('Message = ', self.message)
				return True
			else:
				self.message = re.sub(r"\n", '', re.sub(r"\[(.*?)\]", '', re.sub(r"\((.*?)\)", '', self.raw_text.partition(" ERROR ")[2])))
				self.what_happened = 'Unknown'
				print('Message was not found: ', re.sub(r"\n", '', self.raw_text.partition(" ERROR ")[2]))
				#print('What_happened = ', self.what_happened)
				#print('Message = ', self.message)
				return False

def DatetimeParser(date_time):
	epoch = datetime.utcfromtimestamp(0)
	error_datetime = date_time.partition(",")[0]+'.'+date_time.partition(",")[2][:3]
	error_tz = date_time.partition(",")[2][3:]
	if error_tz == 'Z':
		error_tz = '+0000'

	try:
		dt = datetime.strptime(error_datetime+error_tz, "%Y-%m-%d %H:%M:%S.%f%z").replace(tzinfo=pytz.utc)
		dt = str(int((dt - epoch).total_seconds()))
	except ValueError:
		print(date_time, ': Unknown format')
	return dt

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

def SummarizeErrors(error_who_send, error_what_happened, error_thread, error_text, error_time):
	errors_dict = {}
	for mes_id, err_sender in enumerate(error_who_send):
		if not(err_sender in errors_dict.keys()):
			errors_dict[err_sender] = {}
		reason = error_what_happened[mes_id]
		if reason == 'Unknown':
			m = ''.join(error_text[mes_id])
			if m in errors_dict[err_sender].keys():
				errors_dict[err_sender][m] += 1
			else:
				errors_dict[err_sender][m] = 1
		elif reason in errors_dict[err_sender].keys():
			m = ''.join(error_text[mes_id])
			if m in errors_dict[err_sender][reason].keys():
				errors_dict[err_sender][reason][m] += 1
			else:
				errors_dict[err_sender][reason][m] = 1
		else:
			errors_dict[err_sender][reason] = {}
			m = ''.join(error_text[mes_id])
			if m in errors_dict[err_sender][reason].keys():
				errors_dict[err_sender][reason][m] += 1
			else:
				errors_dict[err_sender][reason][m] = 1
	return errors_dict

def SummarizeErrorsTimeFirst(error_who_send, error_what_happened, error_thread, error_text, error_time):
	error_idx_by_time = sorted(range(len(error_time)), key = lambda k: error_time[k])
	error_text = list(np.asarray(error_text)[error_idx_by_time])
	error_time = list(np.asarray(error_time)[error_idx_by_time])

	errors_dict = {}
	for err_idx, err_time in enumerate(error_time):
		if not(err_time in errors_dict.keys()):
			errors_dict[err_time] = {}
		#TODO
	return errors_dict

def PrintStatistics(logfile_name, errors_dict, errors_num):
	err_info = '________________________________________\n\n'
	err_info += "Summarize statistics for " + logfile_name + '\n'
	err_info += "Number of error messages: " + str(errors_num) + '\n'
	err_info += '________________________________________\n'
	sum_all = []
	for sender in errors_dict.keys():
		sum_sender = []
		for reason in errors_dict[sender].keys():
			sum_reason = 0
			if isinstance(errors_dict[sender][reason], dict):
				for message in errors_dict[sender][reason].keys():
					sum_reason += errors_dict[sender][reason][message]
			else:
				sum_reason += errors_dict[sender][reason]
			sum_sender += [sum_reason]
		sum_all += [sum_sender]

	#print(sum_all)
	for sender_id, sender in enumerate(errors_dict.keys()):
		err_info += "%4d| %s\n" % (np.sum(sum_all[sender_id]), sender)
		for reason_id, reason in enumerate(errors_dict[sender].keys()):
			err_info += "%8d| %s\n" % (sum_all[sender_id][reason_id], reason)
			if isinstance(errors_dict[sender][reason], dict):
				for message_id, message in enumerate(errors_dict[sender][reason].keys()):
					err_info += "%12d| %s\n" % (errors_dict[sender][reason][message], message)
		err_info += '\n'
	return err_info

#def DumpJson(log_names, all_errors):
#	all_data = {}
#	with open('stat_to_plot.js', 'w') as outfile:
#		outfile.write('var errors_data = \n')
#		for log_idx, logname in enumerate(log_names):
#			logname = re.sub('[+\-\s,]', '_', logname)
#			all_data[logname] = all_errors[log_idx]
#		json.dump(all_data, outfile, indent=4, sort_keys=True)
#		outfile.write(';\n')
#		outfile.close()

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Parse logfiles and summarize errors into standart output.')
	parser.add_argument('log_directory', metavar='directory', type=str, nargs=1, help='logfiles directory')
	parser.add_argument('log_filenames', metavar='filename', type=str, nargs='+', help='logfiles filenames (without expansion)')
	parser.add_argument("-o", "--output", help="Directs the output to a name of your choice (with expansion)")
	#parser.add_argument("-js", "--json", action='store_true', help="Create js file with errors statistics in json")
	args = parser.parse_args()

	#if args.json:
	#	all_errors_to_dump = []

	epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
	output_text = ''
	for log in args.log_filenames:
		error_datetime = []
		error_msg_text = []
		error_who_send = []
		error_what_happened = []
		error_thread = []
		print('Analysing', args.log_directory[0] + '/' + log + '.log', '...')
		if not os.path.isfile(args.log_directory[0] + '/' + log + '.log'):
			print("File not found: %s\n" % log+ '.log')
			continue

		with open(args.log_directory[0] + '/' + log + '.log') as f:
			for line in f:
				if 'ERROR' in line:
					e = ErrorLog(line)
					for (method_name, method) in inspect.getmembers(ErrorLog, predicate=inspect.isfunction):
						if 'Parse' in method_name:
							#print('>>', method_name)
							method(e)
					error_datetime += [int(e.date_time.timestamp() * 1000)]
					error_msg_text += [e.message]
					error_who_send += [e.who_send]
					error_what_happened += [e.what_happened]
					error_thread += [e.thread]

		summarize_errors = SummarizeErrors(error_who_send, error_what_happened, error_thread, error_msg_text, error_datetime)
		output_text += PrintStatistics(log, summarize_errors, len(error_datetime))

		#if args.json:
		#	summarize_errors_for_json = SummarizeErrorsTimeFirst(error_who_send, error_what_happened, error_thread, error_msg_text, ''.join(str(x) for x in error_datetime))
		#	output_text += PrintStatistics(log, summarize_errors_for_json, len(error_datetime))
		#	all_errors_to_dump += [summarize_errors_for_json]
	
	#if args.json:
	#	DumpJson(args.log_filenames, all_errors_to_dump)
	if args.output is not None:
		output_file = open(args.output, 'w')
		output_file.write(output_text)
		output_file.close()
	else:
		#print('Here will be output text')
		print(output_text)	