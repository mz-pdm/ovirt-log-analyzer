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
	#event = ''
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
		if ' ERROR ' in self.raw_text:
			mstext = self.raw_text.partition(" ERROR ")[2]
		else:
			mstext = self.raw_text
		template = re.compile(r"Message: (.*)")
		t = re.search(template, mstext)
		if t is not None and len(t.group(1)) > 5:
			t_mes = re.split(r":", t.group(1))
			if len(t_mes) > 1:
				self.event = re.sub(r"\..*", '', t_mes[0])
				self.message = t_mes[1:]
			else:
				t_mes_underlying = re.split(r"message", t_mes[0])
				if len(t_mes_underlying) > 1:
					self.event = t_mes_underlying[0]
					self.message = t_mes_underlying[1:]
				else:
					self.event = 'Unknown'
					self.message = t_mes
			#print('Event = ', self.event)
			#print('MESSAGE = ', self.message)
			return True
		else:
			t_mes = re.split(r"message", mstext)
			if len(t_mes) == 2:
				self.message = [re.sub('[:\{\}\'\n]', '', t_mes[1])]
				self.event = 'Unknown'
				#print('Event = ', self.event)
				#print('Message = ', self.message)
				return True
			template = re.compile(r"(\[.*?\]|\(.*?\)|\{.*?\}|\<.*?\>) *")
			t = re.sub(template, '', mstext)
			t = re.sub('\n', '', t)
			if t is not None and len(t) > 5:
				t_mes = re.split(r":", t)
				if len(t_mes) > 1:
					self.event = t_mes[0]
					self.message = t_mes[1:]
				else:
					t_mes_underlying = re.split(r"message", t_mes[0])
					if len(t_mes_underlying) > 1:
						self.event = t_mes_underlying[0]
						self.message = t_mes_underlying[1:]
					else:
						self.event = 'Unknown'
						self.message = t_mes
				#print('Event = ', self.event)
				#print('Message = ', self.message)
				return True
			else:
				self.message = re.sub(r"\n", '', re.sub(r"\[(.*?)\]", '', re.sub(r"\((.*?)\)", '', mstext)))
				self.event = 'Unknown'
				print('Message was not found: >> ', re.sub(r"\n", '', mstext) + ' <<')
				#print('Event = ', self.event)
				#print('Message = ', self.message)
				return False

#not used
def DatetimeParser(date_time):
	epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
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
#not used
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

def SummarizeErrors(error_who_send, error_event, error_thread, error_text, error_time):
	errors_dict = {}
	errors_datetimes = {}
	for mes_id, err_sender in enumerate(error_who_send):
		if not(err_sender in errors_dict.keys()):
			errors_dict[err_sender] = {}
			errors_datetimes[err_sender] = {}
		event = error_event[mes_id]
		if not event in errors_dict[err_sender].keys():
			errors_dict[err_sender][event] = {}
			errors_datetimes[err_sender][event] = {}
		m = ''.join(error_text[mes_id])
		if m in sorted(errors_dict[err_sender][event].keys()):
			errors_dict[err_sender][event][m] += 1
			errors_datetimes[err_sender][event][m] += [error_time[mes_id]]
		else:
			errors_dict[err_sender][event][m] = 1
			errors_datetimes[err_sender][event][m] = [error_time[mes_id]]
	return errors_dict, errors_datetimes

def SummarizeErrorsTimeFirst(error_who_send, error_event, error_thread, error_text, error_time):
	error_idx_by_time = sorted(range(len(error_time)), key = lambda k: int(error_time[k]))
	error_text = list(np.asarray(error_text)[error_idx_by_time])
	error_time = list(np.asarray(error_time)[error_idx_by_time])
	error_who_send = list(np.asarray(error_who_send)[error_idx_by_time])
	error_event = list(np.asarray(error_event)[error_idx_by_time])
	error_thread = list(np.asarray(error_thread)[error_idx_by_time])
	errors_dict = {}
	for err_idx, err_time in enumerate(error_time):
		if not(err_time in errors_dict.keys()):
			errors_dict[err_time] = {}
			errors_dict[err_time]['number'] = 1
		else:
			errors_dict[err_time]['number'] += 1
		errors_dict[err_time]['sender'] = error_who_send[err_idx]
		errors_dict[err_time]['event'] = error_event[err_idx]
		errors_dict[err_time]['thread'] = error_thread[err_idx]
		#TODO
		errors_dict[err_time]['message'] = ''.join(error_text[err_idx])
	return errors_dict

def PrintStatistics(logfile_name, errors_dict, errors_time, errors_num):
	err_info = '________________________________________\n\n'
	err_info += "Summarized statistics for " + logfile_name + '\n'
	err_info += "Number of error messages: " + str(errors_num) + '\n'
	err_info += '________________________________________\n'
	sum_all = []
	all_time = []
	for sender_id, sender in enumerate(sorted(errors_dict.keys())):
		sum_sender = []
		time_sender = []
		for event_id, event in enumerate(sorted(errors_dict[sender].keys())):
			sum_event = 0
			time_event = []
			for message_id, message in enumerate(sorted(errors_dict[sender][event].keys())):
				sum_event += errors_dict[sender][event][message]
				time_event += [errors_time[sender][event][message]]
			sum_sender += [sum_event]
			time_sender += [time_event]
		sum_all += [sum_sender]
		all_time += [time_sender]
	
	for sender_id, sender in enumerate(sorted(errors_dict.keys())):
		if np.sum(sum_all[sender_id]) < 3:
			str_time = ''.join([datetime.utcfromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y")+'; ' for r in all_time[sender_id] for m in r for m_n in m])
		else:
			str_time = min([datetime.utcfromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for r in all_time[sender_id] for m in r for m_n in m]) + ' - ' + \
						max([datetime.utcfromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for r in all_time[sender_id] for m in r for m_n in m])
		err_info += "%4d| %s\n%s\n" % (np.sum(sum_all[sender_id]), sender, ' '*6+'{'+str_time+'}')
		for event_id, event in enumerate(sorted(errors_dict[sender].keys())):
			if sum_all[sender_id][event_id] < 3:
				str_time = ''.join([datetime.utcfromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y")+'; ' for m in all_time[sender_id][event_id] for m_n in m])
			else:
				str_time = min([datetime.utcfromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for m in all_time[sender_id][event_id] for m_n in m]) + ' - ' + \
							max([datetime.utcfromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for m in all_time[sender_id][event_id] for m_n in m])
			err_info += "%8d| %s\n%s\n" % (sum_all[sender_id][event_id], event, ' '*10+'{'+str_time+'}')
			if isinstance(errors_dict[sender][event], dict):
				for message_id, message in enumerate(sorted(errors_dict[sender][event].keys())):
					if errors_dict[sender][event][message] < 3:
						str_time = ''.join([datetime.utcfromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y")+'; ' for m_n in all_time[sender_id][event_id][message_id]])
					else:
						str_time = min([datetime.utcfromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for m_n in all_time[sender_id][event_id][message_id]]) + ' - ' + \
							max([datetime.utcfromtimestamp(m_n/1000).strftime("%H-%M-%S.%f %d.%m.%Y") for m_n in all_time[sender_id][event_id][message_id]])
					err_info += "%12d| %s\n%s\n" % (errors_dict[sender][event][message], message, ' '*14+'{'+str_time+'}')
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
			timeline[int(date_time[:-3])-min_time//1000] += 1
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
	event_stat = {}
	message_stat = {}
	#thread_stat = {} #firstly add thread to SummarizeErrors
	log_stat = {}
	sender_stat = {}
	event_stat = {}
	message_stat = {}
	for log_id, logname in enumerate(lognames):
		err_log_num = 0
		for sender in sorted(summarized_errors[log_id].keys()):
			err_sender_num = 0
			for event in sorted(summarized_errors[log_id][sender].keys()):
				err_event_num = 0
				for message in sorted(summarized_errors[log_id][sender][event]):
					message_stat[message] = sum(summarized_errors[log_id][sender][event].values())
					err_event_num += sum(summarized_errors[log_id][sender][event].values())
				if not event in event_stat.keys():
					event_stat[event] = err_event_num
				else:
					event_stat[event] += err_event_num
				err_sender_num += err_event_num
			if not sender in sender_stat.keys():
				sender_stat[sender] = err_sender_num
			else:
				sender_stat[sender] += err_sender_num
			err_log_num += err_sender_num
		if not logname in log_stat.keys():
			log_stat[logname] = err_log_num
		else:
			log_stat[logname] += err_log_num
	return log_stat, sender_stat, event_stat, message_stat

def CreateErrorGraph(log_freq, sender_freq, event_freq, message_freq, timeline, d, filename):
	#timeline = np.loadtxt('timeline.txt', delimiter=' ').astype(int)
	#with open('test.json', 'r') as json_data:
	#	d = json.load(json_data)
	fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize = (15,7))
	ax1.plot(np.linspace(0, len(timeline), len(timeline)), timeline, 'b', zorder=1)
	for t in sorted(d.keys(), key=lambda k: int(k)):
		for log in sorted(d[t].keys()):
			if sender_freq[d[t][log]['sender']] < np.median(list(sender_freq.values())):
				s = ax1.scatter(int(t)//1000-min(map(int, list(d.keys())))//1000, d[t][log]['number']+1, s=60, c='orange', marker = '*', zorder=3)
			if message_freq[d[t][log]['message']] < np.median(list(message_freq.values())):
				m = ax1.scatter(int(t)//1000-min(map(int, list(d.keys())))//1000, d[t][log]['number'], s=60, c='red', marker = 'o', zorder=2)
			if event_freq[d[t][log]['event']] < np.median(list(event_freq.values())):
				r = ax1.scatter(int(t)//1000-min(map(int, list(d.keys())))//1000, d[t][log]['number']+2, s=60, c='skyblue', marker = '>', zorder=4)
	ax1.set_title('Suspicious errors')
	ax1.set_xlabel('Timeline (step=1s)')
	ax1.set_ylabel('Number of errors')
	ax1.legend((m, s, r), ('Rare messages', 'Rare senders', 'Rare events'), loc=1)
	G=nx.DiGraph()
	for t in sorted(d.keys(), key=lambda k: int(k)):
		for log in sorted(d[t].keys()):
			if sender_freq[d[t][log]['sender']] < np.median(list(sender_freq.values())):
				G.add_node(datetime.utcfromtimestamp(int(t)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y")+'\nSender=('+d[t][log]['sender']+\
								')\nEvent=['+d[t][log]['event']+']\n'+'Suspicious because of sender', \
							time=datetime.utcfromtimestamp(int(t)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"),\
							freq=sender_freq[d[t][log]['sender']], link_type='sender', \
							thread=d[t][log]['thread'], sender=d[t][log]['sender'], event=d[t][log]['event'], \
							message=d[t][log]['message'], colorscheme='rdylbu4', style='filled', fillcolor=2)
			if message_freq[d[t][log]['message']] < np.median(list(message_freq.values())):
				G.add_node(datetime.utcfromtimestamp(int(t)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y")+'\nSender=('+d[t][log]['sender']+\
								')\nEvent=['+d[t][log]['event']+']\n'+'Suspicious because of message', \
							time=datetime.utcfromtimestamp(int(t)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"),\
							freq=message_freq[d[t][log]['message']], link_type='message', \
							thread=d[t][log]['thread'], sender=d[t][log]['sender'], event=d[t][log]['event'], \
							message=d[t][log]['message'], colorscheme='rdylbu4', style='filled', fillcolor=1)
			if event_freq[d[t][log]['event']] < np.median(list(event_freq.values())):
				G.add_node(datetime.utcfromtimestamp(int(t)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y")+'\nSender=('+d[t][log]['sender']+\
								')\nEvent=['+d[t][log]['event']+']\n'+'Suspicious because of event', \
							time=datetime.utcfromtimestamp(int(t)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"),\
							freq=event_freq[d[t][log]['event']], link_type='event', \
							thread=d[t][log]['thread'], sender=d[t][log]['sender'], event=d[t][log]['event'], \
							message=d[t][log]['message'], colorscheme='rdylbu4', style='filled', fillcolor=3)
	G_copy = G.copy()
	for t_susp, attr in sorted(G_copy.nodes(data=True), key=lambda k: k[1]['time']):
		#check if mean number of following errors is greater than previos (look over 5 seconds)
		t_su = str(int(datetime.strptime(attr['time'], "%H-%M-%S.%f\n%d.%m.%Y").replace(tzinfo=pytz.utc).timestamp() * 1000))
		step = 20
		count_err_next = np.mean(timeline[1+int(t_su)//1000-min(map(int, d.keys()))//1000:]) if int(t_su)//1000-min(map(int, d.keys()))//1000+step+1 > len(timeline) \
							else np.mean(timeline[1+int(t_su)//1000-min(map(int, d.keys()))//1000:int(t_su)//1000-min(map(int, d.keys()))//1000+step+1])
		count_err_prev = np.mean(timeline[0:int(t_su)//1000-min(map(int, d.keys()))//1000]) if int(t_su)//1000-min(map(int, d.keys()))//1000-step < 0 \
							else np.mean(timeline[int(t_su)//1000-min(map(int, d.keys()))//1000-step:int(t_su)//1000-min(map(int, d.keys()))//1000])
		next_point = max(map(int, d.keys())) if int(t_su)//1000-min(map(int, d.keys()))//1000+step+1 > len(timeline) \
							else int(t_su)//1000+step+1
		added_messages = []
		added_events = []
		prev_error = None
		link_message = ''
		if count_err_next > count_err_prev:
			for t_err in sorted(d.keys(), key=lambda k: int(k)):
				for log in sorted(d[t_err].keys()):
					if (int(t_su)//1000 < int(t_err)//1000 < next_point):
						#if not (d[t_err][log]['message'] in added_messages or d[t_err][log]['event'] in added_events):
						if prev_error is not None:
							err_to_link = prev_error
							link_label = link_message
						else:
							err_to_link = datetime.utcfromtimestamp(int(t_su)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y")+'\nSender=('+attr['sender']+\
										')\nEvent=['+attr['event']+']\n'+'Suspicious because of '+ attr['link_type']
							link_label = attr['message']
						G.add_node(datetime.utcfromtimestamp(int(t_err)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y")+'\nSender=('+d[t_err][log]['sender']+\
										')\nEvent=['+d[t_err][log]['event']+']\n'+'Suspicious because of time', \
									time=datetime.utcfromtimestamp(int(t_err)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"),\
									freq = (count_err_next-count_err_prev)*2, link_type = 'time', \
									thread = d[t_err][log]['thread'], sender = d[t_err][log]['sender'], event = d[t_err][log]['event'], \
									message = d[t_err][log]['message'], colorscheme='rdylbu4', style='filled', fillcolor=4)
						G.add_edge(err_to_link, \
									datetime.utcfromtimestamp(int(t_err)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y")+'\nSender=('+d[t_err][log]['sender']+\
									')\nEvent=['+d[t_err][log]['event']+']\n'+'Suspicious because of time', \
									label = link_label)													
						#added_messages += [d[t_err][log]['message']]
						#added_events += [d[t_err][log]['event']]
						prev_error = datetime.utcfromtimestamp(int(t_err)/1000).strftime("%H-%M-%S.%f\n%d.%m.%Y")+'\nSender=('+d[t_err][log]['sender']+\
										')\nEvent=['+d[t_err][log]['event']+']\n'+'Suspicious because of time'
						link_message = d[t_err][log]['message']
						#else:
						#	continue
		else:
			prev_error = None
			link_message = ''
	need_nodes = set([])
	for edge in G.edges_iter():
		need_nodes |= set(edge)
	G = G.subgraph(need_nodes)
	color_map = {'message':'r', 'sender':'orange', 'event':'skyblue', 'time':'b'} 
	nx.draw(G, node_color=[color_map[G.node[node]['link_type']] for node in G], node_size=[G.node[node]['freq']*100 for node in G])
	ax2.set_title('Linked errors (see full version in pdf)')
	fig.savefig('plt_'+ filename +'.png')
	nx.drawing.nx_pydot.write_dot(G, filename+".dot")

def LoopOverLines(logname):
	error_datetime = []
	error_msg_text = []
	error_who_send = []
	error_event = []
	error_thread = []
	with open(logname) as f:
		error_info = {}
		error_traceback = ''
		in_traceback_flag = False
		for line in f:
			if any(w in line for w in ['INFO', 'DEBUG', 'ERROR']):
				if error_info:
					#print(line, '\n', error_info, '\n')
					error_datetime += [error_info['datetime']]
					error_who_send += [error_info['who_send']]
					error_thread += [error_info['thread']]
					if not in_traceback_flag:
						error_event += [error_info['event']]
						error_msg_text += [error_info['msg_text']]
					else:
						traceback_message = ErrorLog(error_traceback)
						traceback_message.ParseMessage()
						error_event += [traceback_message.event]
						error_msg_text += [traceback_message.message]
				error_info = {}
				error_traceback = ''
				in_traceback_flag = False
				if 'ERROR' in line:
					e = ErrorLog(line)
					for (method_name, method) in inspect.getmembers(ErrorLog, predicate=inspect.isfunction):
						if 'Parse' in method_name:
							method(e)
					error_info['datetime'] = int(e.date_time.timestamp() * 1000)
					error_info['who_send'] = e.who_send
					error_info['thread'] = e.thread
					error_info['event'] = e.event
					error_info['msg_text'] = e.message
			elif 'Traceback' in line:
				in_traceback_flag = True
				error_traceback = line
			elif in_traceback_flag and not line == '':
				error_traceback = line
		#adding the last error
		if 'ERROR' in line or in_traceback_flag:
			error_datetime += [error_info['datetime']]
			error_who_send += [error_info['who_send']]
			error_thread += [error_info['thread']]
			if not in_traceback_flag:
				error_event += [error_info['event']]
				error_msg_text += [error_info['msg_text']]
			else:
				error_event += [error_traceback['event']]
				error_msg_text += [error_traceback['msg_text']]
	return error_datetime, error_who_send, error_thread, error_event, error_msg_text

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
	found_lognames = []
	output_text = ''

	for log in args.log_filenames:
		print('Analysing', args.log_directory[0] + '/' + log + '.log', '...')
		if not os.path.isfile(args.log_directory[0] + '/' + log + '.log'):
			print("File not found: %s\n" % log+ '.log')
			continue
		#save name of actually opened logfile
		found_lognames += [log]

		#gathering all information about errors from a logfile into lists
		error_datetime, error_who_send, error_thread, error_event, error_msg_text = LoopOverLines(args.log_directory[0] + '/' + log + '.log')
		#calculating a number of every error's appearing in a file and its time
		summarize_errors, summarize_errors_time = SummarizeErrors(error_who_send, error_event, error_thread, error_msg_text, error_datetime)
		#saving them to a list with other log files' errors
		all_errors_statistics += [summarize_errors]
		all_errors_statistics_time += [summarize_errors_time]
		#creating an output string, calculating a number of appearing of every sender, thread, event
		output_text += PrintStatistics(log, summarize_errors, summarize_errors_time, len(error_datetime))
		
		#calculating a number of errors for every moment of time (needed for chart and graph)
		summarize_errors_time_first = SummarizeErrorsTimeFirst(error_who_send, error_event, error_thread, error_msg_text, [str(x) for x in error_datetime])
		#saving it to a list with other log files' errors' time
		all_errors_time_first += [summarize_errors_time_first]
	
	#Linking related errors to each other
	if args.dot is not None:
		#summarizing errors' appearing number like in PrintStatistics
		log_freq, sender_freq, event_freq, message_freq = CalculateErrorsFrequency(found_lognames, all_errors_statistics)
		#summarizing errors' appearing number like in PrintStatistics
		timeline, errors_dict = MergeAllErrorsByTime(found_lognames, all_errors_time_first)
		#searching for suspicious errors, linking them with following errors
		CreateErrorGraph(log_freq, sender_freq, event_freq, message_freq, timeline, errors_dict, args.dot)

	#Saving error's info by time (for a chart)
	if args.json:
		DumpJson(found_lognames, all_errors_time_first)
	
	#Saving to a specified file
	if args.output is not None:
		output_file = open(args.output, 'w')
		output_file.write(output_text)
		output_file.close()
	else:
		#print('Here will be output text')
		print(output_text)	