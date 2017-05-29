"""Linking errors from logfiles to each other
"""
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
from datetime import datetime
import pytz


def create_error_graph(log_freq, sender_freq, event_freq, message_freq, 
                            timeline, d, filename, step):
    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(30, 15))
    ax1.plot(np.linspace(0, len(timeline), len(timeline)),
             timeline, 'b', zorder=1)
    for t in sorted(d.keys(), key=lambda k: int(k)):
        for log in sorted(d[t].keys()):
            if sender_freq[d[t][log]['sender']] <= np.median(list(
                                                    sender_freq.values())):
                s = ax1.scatter(int(t) // 1000 - \
                                        min(map(int, list(d.keys()))) // 1000, \
                                            d[t][log]['number'] + 1, 
                                s=60, 
                                c='orange', 
                                marker='*', 
                                zorder=3)
            if message_freq[d[t][log]['message']] <= np.median(list(
                                                    message_freq.values())):
                m = ax1.scatter(int(t) // 1000 - \
                                        min(map(int, list(d.keys()))) // 1000, \
                                            d[t][log]['number'], 
                                s=60, 
                                c='red', 
                                marker='o', 
                                zorder=2)
            if event_freq[d[t][log]['event']] <= np.median(list(
                                                event_freq.values())):
                r = ax1.scatter(int(t) // 1000 - \
                                        min(map(int, list(d.keys()))) // 1000, \
                                            d[t][log]['number'] + 2, 
                                s=60, 
                                c='skyblue', 
                                marker='>', 
                                zorder=4)
    ax1.set_title('Suspicious errors')
    ax1.set_xlabel('Timeline (step=1s)')
    ax1.set_ylabel('Number of errors')
    ax1.legend((m, s, r), ('Rare messages',
                           'Rare senders', 'Rare events'), loc=1)
    G = nx.DiGraph()
    for t in sorted(d.keys(), key=lambda k: int(k)):
        for log in sorted(d[t].keys()):
            if sender_freq[d[t][log]['sender']] < np.median(list(
                                                    sender_freq.values())):
                G.add_node(datetime.utcfromtimestamp(int(t) / 1000).strftime(
                                                    "%H-%M-%S.%f\n%d.%m.%Y") + \
                           '\nSender=(' + d[t][log]['sender'] + \
                           ')\nEvent=[' + d[t][log]['event'] + ']\n' + \
                           'Suspicious because of sender',
                           time=datetime.utcfromtimestamp(
                               int(t) / 1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"),
                           freq=sender_freq[d[t][log]['sender']],
                           link_type='sender',
                           thread=d[t][log]['thread'],
                           sender=d[t][log]['sender'],
                           event=d[t][log]['event'],
                           message=d[t][log]['message'],
                           colorscheme='rdylbu4',
                           style='filled',
                           fillcolor=2)
            if message_freq[d[t][log]['message']] < np.median(list(
                                                    message_freq.values())):
                G.add_node(datetime.utcfromtimestamp(int(t) / 1000).strftime(
                                                    "%H-%M-%S.%f\n%d.%m.%Y") + \
                           '\nSender=(' + d[t][log]['sender'] + \
                           ')\nEvent=[' + d[t][log]['event'] + ']\n' + \
                           'Suspicious because of message',
                           time=datetime.utcfromtimestamp(
                               int(t) / 1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"),
                           freq=message_freq[d[t][log]['message']],
                           link_type='message',
                           thread=d[t][log]['thread'],
                           sender=d[t][log]['sender'],
                           event=d[t][log]['event'],
                           message=d[t][log]['message'],
                           colorscheme='rdylbu4',
                           style='filled',
                           fillcolor=1)
            if event_freq[d[t][log]['event']] < np.median(list(
                                                event_freq.values())):
                G.add_node(datetime.utcfromtimestamp(int(t) / 1000).strftime(
                                                    "%H-%M-%S.%f\n%d.%m.%Y") + \
                           '\nSender=(' + d[t][log]['sender'] + \
                           ')\nEvent=[' + d[t][log]['event'] + \
                           ']\n' + 'Suspicious because of event',
                           time=datetime.utcfromtimestamp(
                               int(t) / 1000).strftime("%H-%M-%S.%f\n%d.%m.%Y"),
                           freq=event_freq[d[t][log]['event']],
                           link_type='event',
                           thread=d[t][log]['thread'],
                           sender=d[t][log]['sender'],
                           event=d[t][log]['event'],
                           message=d[t][log]['message'],
                           colorscheme='rdylbu4',
                           style='filled',
                           fillcolor=3)
    G_copy = G.copy()
    for t_susp, attr in sorted(G_copy.nodes(data=True), 
                                key=lambda k: int(datetime.strptime(k[1]['time'], 
                                            "%H-%M-%S.%f\n%d.%m.%Y").replace(
                                        tzinfo=pytz.utc).timestamp() * 1000)):
        # check if mean number of following errors is greater than previos
        # (look over 5 seconds)
        t_su = str(int(datetime.strptime(attr['time'], 
                        "%H-%M-%S.%f\n%d.%m.%Y").replace(
                                        tzinfo=pytz.utc).timestamp() * 1000))
        #step = 1
        count_err_next = np.mean(timeline[int(t_su) // 1000 - \
                                    min(map(int, d.keys())) // 1000:]) \
                            if int(t_su) // 1000 - \
                                min(map(int, d.keys())) // 1000 + step + 1 \
                                >= len(timeline) \
                            else np.mean(timeline[1 + int(t_su) // 1000 - \
                                        min(map(int, d.keys())) // \
                                        1000:int(t_su) // 1000 - \
                                        min(map(int, d.keys())) // 1000 \
                                        + step + 1])
        count_err_prev = np.mean(timeline[0:1 + int(t_su) // 1000 - \
                                    min(map(int, d.keys())) // 1000]) \
                        if int(t_su) // 1000 - \
                            min(map(int, d.keys())) // 1000 - step \
                            <= 0 \
                        else np.mean(timeline[int(t_su) // 1000 - \
                                        min(map(int, d.keys())) // 1000 - \
                                        step:int(t_su) // 1000 - \
                                        min(map(int, d.keys())) // 1000])
        next_point = max(map(int, d.keys())) \
                        if int(t_su) // 1000 - \
                            min(map(int, d.keys())) // 1000 + step + 1 \
                            >= len(timeline) \
                        else int(t_su) // 1000 + step + 1
        added_messages = []
        added_events = []
        prev_error = None
        link_message = ''
        if count_err_next > count_err_prev:
            for t_err in sorted(d.keys(), key=lambda k: int(k)):
                for log in sorted(d[t_err].keys()):
                    if (int(t_su) // 1000 < int(t_err) // 1000 < next_point):
                        if prev_error is not None:
                            err_to_link = prev_error
                            link_label = link_message
                        else:
                            err_to_link = datetime.utcfromtimestamp(int(t_su)\
                                            / 1000).strftime( \
                                            "%H-%M-%S.%f\n%d.%m.%Y") +  \
                                            '\nSender=(' + attr['sender'] + \
                                    ')\nEvent=[' + attr['event'] + ']\n' + \
                                    'Suspicious because of ' + attr['link_type']
                            link_label = attr['message']
                        G.add_node(datetime.utcfromtimestamp(int(t_err) \
                                            / 1000).strftime( \
                                            "%H-%M-%S.%f\n%d.%m.%Y") + \
                                            '\nSender=(' + d[t_err][log][ \
                                            'sender'] + ')\nEvent=[' + \
                                            d[t_err][log]['event'] + ']\n' + \
                                            'Suspicious because of time',
                                   time=datetime.utcfromtimestamp(
                                       int(t_err) / 1000).strftime( \
                                                    "%H-%M-%S.%f\n%d.%m.%Y"),
                                   freq=(count_err_next - count_err_prev) * 2,
                                   link_type='time',
                                   thread=d[t_err][log]['thread'],
                                   sender=d[t_err][log]['sender'],
                                   event=d[t_err][log]['event'],
                                   message=d[t_err][log]['message'],
                                   colorscheme='rdylbu4',
                                   style='filled',
                                   fillcolor=4)
                        G.add_edge(err_to_link,
                                   datetime.utcfromtimestamp(int(t_err) \
                                            / 1000).strftime(\
                                            "%H-%M-%S.%f\n%d.%m.%Y") + \
                                            '\nSender=(' + \
                                            d[t_err][log]['sender'] + \
                                   ')\nEvent=[' + d[t_err][log]['event'] +
                                   ']\n' + 'Suspicious because of time',
                                   label=link_label)
                        prev_error = datetime.utcfromtimestamp(int(t_err) \
                                                / 1000).strftime(\
                                                "%H-%M-%S.%f\n%d.%m.%Y") + \
                                                '\nSender=(' + \
                                                d[t_err][log]['sender'] + \
                                    ')\nEvent=[' + d[t_err][log]['event'] + \
                                    ']\n' + 'Suspicious because of time'
                        link_message = d[t_err][log]['message']
        else:
            prev_error = None
            link_message = ''
    need_nodes = set([])
    for edge in G.edges_iter():
        need_nodes |= set(edge)
    G = G.subgraph(need_nodes)
    color_map = {'message': 'r', 'sender': 'orange',
                 'event': 'skyblue', 'time': 'b'}
    nx.draw(G, node_color=[color_map[G.node[node]['link_type']] \
                           for node in G], \
                node_size=[G.node[node]['freq'] * 100 for node in G])
    ax2.set_title('Linked errors (see full version in pdf)')
    fig.savefig(filename + '_plt.png')
    nx.drawing.nx_pydot.write_dot(G, filename + ".dot")
