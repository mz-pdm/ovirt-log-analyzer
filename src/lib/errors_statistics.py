"""Linking errors from logfiles to each other
This module contains methods of collecting errors' statistics
	
	For one logfile:
	- summarize_errors - dictionary with the following structure (embedded keys): \
		sender - event - message - [number of messages]
	- summarize_errors_time_first - dictionary with the following structure (embedded keys): \
		time - [sender, thread, event, message], [number of messages]
	
	For all logfiles:
	- merge_all_errors_by_time - returns timeline (number of errors in each millisecond) \
		and errors from all logfiles:
		time - logfile - [sender, thread, event, message]
	- calculate_frequency - calculates number of occurance of each \
		sender, event, message and generally number of errors in the logfile
"""
import numpy as np

def summarize_errors(error_who_send, error_event, error_thread, error_text, error_time):
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

def summarize_errors_time_first(error_who_send, error_event, error_thread, error_text, error_time):
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

def merge_all_errors_by_time(lognames, summarized_errors_time_first):
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
	return timeline, all_errors

def calculate_frequency(lognames, summarized_errors):
	sender_stat = {}
	event_stat = {}
	message_stat = {}
	#thread_stat = {} #firstly add thread to SummarizeErrors
	log_stat = {}
	sender_stat = {}
	event_stat = {}
	message_stat = {}
	for logname in lognames:
		err_log_num = 0
		for sender in sorted(summarized_errors[logname].keys()):
			err_sender_num = 0
			for event in sorted(summarized_errors[logname][sender].keys()):
				err_event_num = 0
				for message in sorted(summarized_errors[logname][sender][event]):
					message_stat[message] = sum(summarized_errors[logname][sender][event].values())
					err_event_num += sum(summarized_errors[logname][sender][event].values())
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