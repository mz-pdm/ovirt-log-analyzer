import numpy as np
import os
import argparse
import re
import dateutil.parser
import pytz
import json
import time
import inspect
import networkx as nx
import matplotlib.pyplot as plt
from pytz import timezone
from datetime import datetime, date
from pytz import all_timezones
from optparse import OptionParser

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
	errors_datetimes = {}
	for mes_id, err_sender in enumerate(error_who_send):
		if not(err_sender in errors_dict.keys()):
			errors_dict[err_sender] = {}
			errors_datetimes[err_sender] = {}
		reason = error_what_happened[mes_id]
		if not reason in errors_dict[err_sender].keys():
			errors_dict[err_sender][reason] = {}
			errors_datetimes[err_sender][reason] = {}
		m = ''.join(error_text[mes_id])
		if m in errors_dict[err_sender][reason].keys():
			errors_dict[err_sender][reason][m] += 1
			errors_datetimes[err_sender][reason][m] += [error_time[mes_id]]
		else:
			errors_dict[err_sender][reason][m] = 1
			errors_datetimes[err_sender][reason][m] = [error_time[mes_id]]
	return errors_dict, errors_datetimes

def SummarizeErrorsTimeFirst(error_who_send, error_what_happened, error_thread, error_text, error_time):
	error_idx_by_time = sorted(range(len(error_time)), key = lambda k: error_time[k])
	error_text = list(np.asarray(error_text)[error_idx_by_time])
	error_time = list(np.asarray(error_time)[error_idx_by_time])
	errors_dict = {}
	for err_idx, err_time in enumerate(error_time):
		if not(err_time in errors_dict.keys()):
			errors_dict[err_time] = {}
			errors_dict[err_time]['number'] = 1
		else:
			errors_dict[err_time]['number'] += 1
		errors_dict[err_time]['sender'] = error_who_send[err_idx]
		errors_dict[err_time]['what_happened'] = error_what_happened[err_idx]
		errors_dict[err_time]['thread'] = error_thread[err_idx]
		#TODO
		errors_dict[err_time]['message'] = ''.join(error_text[err_idx])
	return errors_dict

def PrintStatistics(logfile_name, errors_dict, errors_time, errors_num):
	err_info = '________________________________________\n\n'
	err_info += "Summarize statistics for " + logfile_name + '\n'
	err_info += "Number of error messages: " + str(errors_num) + '\n'
	err_info += '________________________________________\n'
	sum_all = []
	all_time = []
	for sender_id, sender in enumerate(sorted(errors_dict.keys())):
		sum_sender = []
		time_sender = []
		for reason_id, reason in enumerate(sorted(errors_dict[sender].keys())):
			sum_reason = 0
			time_reason = []
			for message_id, message in enumerate(sorted(errors_dict[sender][reason].keys())):
				sum_reason += errors_dict[sender][reason][message]
				time_reason += [errors_time[sender][reason][message]]
			sum_sender += [sum_reason]
			time_sender += [time_reason]
		sum_all += [sum_sender]
		all_time += [time_sender]
	
	for sender_id, sender in enumerate(sorted(errors_dict.keys())):
		if np.sum(sum_all[sender_id]) < 3:
			str_time = ''.join([datetime.fromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y")+'; ' for r in all_time[sender_id] for m in r for m_n in m])
		else:
			str_time = min([datetime.fromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for r in all_time[sender_id] for m in r for m_n in m]) + ' - ' + \
						max([datetime.fromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for r in all_time[sender_id] for m in r for m_n in m])
		err_info += "%4d| %s\n%s\n" % (np.sum(sum_all[sender_id]), sender, ' '*6+'{'+str_time+'}')
		for reason_id, reason in enumerate(sorted(errors_dict[sender].keys())):
			if sum_all[sender_id][reason_id] < 3:
				str_time = ''.join([datetime.fromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y")+'; ' for m in all_time[sender_id][reason_id] for m_n in m])
			else:
				str_time = min([datetime.fromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for m in all_time[sender_id][reason_id] for m_n in m]) + ' - ' + \
							max([datetime.fromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for m in all_time[sender_id][reason_id] for m_n in m])
			err_info += "%8d| %s\n%s\n" % (sum_all[sender_id][reason_id], reason, ' '*10+'{'+str_time+'}')
			if isinstance(errors_dict[sender][reason], dict):
				for message_id, message in enumerate(errors_dict[sender][reason].keys()):
					if errors_dict[sender][reason][message] < 3:
						str_time = ''.join([datetime.fromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y")+'; ' for m_n in all_time[sender_id][reason_id][message_id]])
					else:
						str_time = min([datetime.fromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for m_n in all_time[sender_id][reason_id][message_id]]) + ' - ' + \
							max([datetime.fromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for m_n in all_time[sender_id][reason_id][message_id]])
					err_info += "%12d| %s\n%s\n" % (errors_dict[sender][reason][message], message, ' '*14+'{'+str_time+'}')
		err_info += '\n'
	return err_info

def DumpJson(log_names, all_errors):
	all_data = {}
	with open('stat_to_plot.js', 'w') as outfile:
		outfile.write('var errors_data = \n')
		for log_idx, logname in enumerate(log_names):
			logname = re.sub('[+\-\s,]', '_', logname)
			all_data[logname] = all_errors[log_idx]
		json.dump(all_data, outfile, indent=4, sort_keys=True)
		outfile.write(';\n')
		outfile.close()

def MergeAllErrorsByTime(lognames, summarized_errors_time_first):
	err_time = {}
	all_times = [t for log in summarized_errors_time_first for t in log]
	min_time = min(map(int, all_times))
	max_time = max(map(int, all_times))

	timeline = np.zeros((max_time//1000 - min_time//1000+1))
	all_errors = {}
	for log_id in range(len(summarized_errors_time_first)):
		for date_time in summarized_errors_time_first[log_id]:
			timeline[max_time//1000-int(date_time[:-3])] += 1
			if not date_time in all_errors.keys():
				all_errors[date_time] = {}
			all_errors[date_time][lognames[log_id]] = summarized_errors_time_first[log_id][date_time]

	#Temporally saving to file, will be removed
	#np.savetxt('timeline.txt', timeline, delimiter=' ')
	#outfile = open('test.json', 'w')
	#json.dump(all_errors, outfile, indent=4, sort_keys=True)
	#outfile.close()
	return timeline, all_errors

def CalculateErrorsFrequency(lognames, summarized_errors):
	sender_stat = {}
	reason_stat = {}
	message_stat = {}
	#thread_stat = {} #firstly add thread to SummarizeErrors
	log_stat = {}
	sender_stat = {}
	reason_stat = {}
	message_stat = {}
	for log_id, logname in enumerate(lognames):
		err_log_num = 0
		for sender in summarized_errors[log_id].keys():
			err_sender_num = 0
			for reason in summarized_errors[log_id][sender].keys():
				err_reason_num = 0
				for message in summarized_errors[log_id][sender][reason]:
					message_stat[message] = sum(summarized_errors[log_id][sender][reason].values())
					err_reason_num += sum(summarized_errors[log_id][sender][reason].values())
				if not reason in reason_stat.keys():
					reason_stat[reason] = err_reason_num
				else:
					reason_stat[reason] += err_reason_num
				err_sender_num += err_reason_num
			if not sender in sender_stat.keys():
				sender_stat[sender] = err_sender_num
			else:
				sender_stat[sender] += err_sender_num
			err_log_num += err_sender_num
		if not logname in log_stat.keys():
			log_stat[logname] = err_log_num
		else:
			log_stat[logname] += err_log_num
	return log_stat, sender_stat, reason_stat, message_stat

def CreateErrorGraph(log_freq, sender_freq, reason_freq, message_freq, timeline, d, filename):
	#timeline = np.loadtxt('timeline.txt', delimiter=' ').astype(int)
	#with open('test.json', 'r') as json_data:
	#	d = json.load(json_data)
	fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize = (15,7))
	ax1.plot(np.linspace(0, len(timeline), len(timeline)), timeline, 'b', zorder=1)
	for t in d.keys():
		for log in d[t].keys():
			if message_freq[d[t][log]['message']] < np.median(list(message_freq.values())):
				m = ax1.scatter(max(map(int, list(d.keys())))//1000-int(t)//1000, d[t][log]['number'], s=60, c='r', marker = 'o', zorder=2)
			if sender_freq[d[t][log]['sender']] < np.median(list(sender_freq.values())):
				s = ax1.scatter(max(map(int, list(d.keys())))//1000-int(t)//1000, d[t][log]['number']+1, s=60, c='g', marker = '*', zorder=3)
			if reason_freq[d[t][log]['what_happened']] < np.median(list(reason_freq.values())):
				r = ax1.scatter(max(map(int, list(d.keys())))//1000-int(t)//1000, d[t][log]['number']+2, s=60, c='y', marker = '>', zorder=4)
	ax1.set_title('Suspicious errors')
	ax1.set_xlabel('Timeline (step=1s)')
	ax1.set_ylabel('Number of errors')
	ax1.legend((m, s, r), ('Rare messages', 'Rare senders', 'Rare events'), loc=2)

	G=nx.DiGraph()
	for t_id, t in enumerate(d.keys()):
		for log in d[t].keys():
			if sender_freq[d[t][log]['sender']] < np.median(list(sender_freq.values())):
				G.add_node(datetime.fromtimestamp(int(t)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"), freq=sender_freq[d[t][log]['sender']], link_type='sender', \
							thread=d[t][log]['thread'], sender=d[t][log]['sender'], reason=d[t][log]['what_happened'], \
							message=d[t][log]['message'], colorscheme='rdylbu4', style='filled', fillcolor=2)
			if message_freq[d[t][log]['message']] < np.median(list(message_freq.values())):
				G.add_node(datetime.fromtimestamp(int(t)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"), freq=message_freq[d[t][log]['message']], link_type='message', \
							thread=d[t][log]['thread'], sender=d[t][log]['sender'], reason=d[t][log]['what_happened'], \
							message=d[t][log]['message'], colorscheme='rdylbu4', style='filled', fillcolor=1)
			if reason_freq[d[t][log]['what_happened']] < np.median(list(reason_freq.values())):
				G.add_node(datetime.fromtimestamp(int(t)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"), freq=reason_freq[d[t][log]['what_happened']], link_type='reason', \
							thread=d[t][log]['thread'], sender=d[t][log]['sender'], reason=d[t][log]['what_happened'], \
							message=d[t][log]['message'], colorscheme='rdylbu4', style='filled', fillcolor=3)

	for t_susp, attr in G.nodes(data=True):
		#check if mean number of following errors is greater than previos (look over 5 seconds)
		t_susp = str(int(datetime.strptime(t_susp, "%H-%M-%S.%f\n%d.%m.%Y").timestamp() * 1000))
		step = 2
		count_err_next = np.mean(timeline[max(map(int, d.keys()))//1000-int(t)//1000:-1]) if max(map(int, d.keys()))//1000-int(t)//1000+step > len(timeline) \
							else np.mean(timeline[max(map(int, d.keys()))//1000-int(t)//1000:max(map(int, d.keys()))//1000-int(t)//1000+step])
		count_err_prev = np.mean(timeline[0:max(map(int, d.keys()))//1000-int(t)//1000]) if max(map(int, d.keys()))//1000-int(t)//1000-step < 0 \
							else np.mean(timeline[max(map(int, d.keys()))//1000-int(t)//1000-step:max(map(int, d.keys()))//1000-int(t)//1000])
		next_point = max(map(int, d.keys()))//1000 if max(map(int, d.keys()))//1000-int(t)//1000+step > len(timeline) else max(map(int, d.keys()))//1000+step
		added_messages = []
		added_reasons = []
		prev_error = None
		if count_err_next > count_err_prev:
			for t_err in sorted(d.keys(), key=lambda k: int(k)):
				for log in d[t_err].keys():
					if (int(t_susp)//1000 < int(t_err)//1000 < next_point):
						if not (d[t_err][log]['message'] in added_messages or d[t_err][log]['what_happened'] in added_reasons):
							if prev_error is not None:
								err_to_link = prev_error
							else:
								err_to_link = t_susp
							G.add_node(datetime.fromtimestamp(int(t_err)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"), freq = (count_err_next-count_err_prev)*2, link_type = 'time', \
										thread = d[t_err][log]['thread'], sender = d[t_err][log]['sender'], reason = d[t_err][log]['what_happened'], \
										message = d[t_err][log]['message'], colorscheme='rdylbu4', style='filled', fillcolor=4)
							G.add_edge(datetime.fromtimestamp(int(err_to_link)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"), \
										datetime.fromtimestamp(int(t_err)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"))
							added_messages += [d[t_err][log]['message']]
							added_reasons += [d[t_err][log]['what_happened']]
							prev_error = t_err
						else:
							continue
		else:
			prev_error = None
	color_map = {'message':'r', 'sender':'orange', 'reason':'skyblue', 'time':'b'} 
	nx.draw(G, with_labels=True, node_color=[color_map[G.node[node]['link_type']] for node in G], node_size=[G.node[node]['freq']*100 for node in G])
	ax2.set_title('Linked errors (see full version in pdf)')
	fig.savefig('plt_'+ filename +'.png')
	nx.drawing.nx_pydot.write_dot(G, filename+".dot")

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Parse logfiles and summarize errors into standart output.')
	parser.add_argument('log_directory', metavar='directory', type=str, nargs=1, help='logfiles directory')
	parser.add_argument('log_filenames', metavar='filename', type=str, nargs='+', help='logfiles filenames (without expansion)')
	parser.add_argument("-o", "--output", help="Directs the output to a name of your choice (with expansion)")
	parser.add_argument("-js", "--json", action='store_true', help="Create js file with errors statistics in json")
	parser.add_argument("-g", "--dot", help="Create dot file and pdf with graph of linked errors")
	args = parser.parse_args()

	all_errors_statistics = []
	all_errors_statistics_time = []
	all_errors_time_first = []

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
							method(e)
					error_datetime += [int(e.date_time.timestamp() * 1000)]
					error_msg_text += [e.message]
					error_who_send += [e.who_send]
					error_what_happened += [e.what_happened]
					error_thread += [e.thread]

		summarize_errors, summarize_errors_time = SummarizeErrors(error_who_send, error_what_happened, error_thread, error_msg_text, error_datetime)
		all_errors_statistics += [summarize_errors]
		all_errors_statistics_time += [summarize_errors_time]
		output_text += PrintStatistics(log, summarize_errors, summarize_errors_time, len(error_datetime))

		summarize_errors_time_first = SummarizeErrorsTimeFirst(error_who_send, error_what_happened, error_thread, error_msg_text, [str(x) for x in error_datetime])
		all_errors_time_first += [summarize_errors_time_first]
	
	#Linking related errors to each other
	if args.dot is not None:
		log_freq, sender_freq, reason_freq, message_freq = CalculateErrorsFrequency(args.log_filenames, all_errors_statistics)
		timeline, errors_dict = MergeAllErrorsByTime(args.log_filenames, all_errors_time_first)
		CreateErrorGraph(log_freq, sender_freq, reason_freq, message_freq, timeline, errors_dict, args.dot)

	if args.json:
		DumpJson(args.log_filenames, all_errors_time_first)
	if args.output is not None:
		output_file = open(args.output, 'w')
		output_file.write(output_text)
		output_file.close()
	else:
		#print('Here will be output text')
		print(output_text)	