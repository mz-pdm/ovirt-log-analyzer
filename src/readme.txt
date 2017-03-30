Command to run:

python summarize_logs.py ovirtlogs engine vdsm-1 vdsm-2 -o out.txt -js -d graph

To plot error graph via graphviz:
dot -tPdf graph.dot -o graph.pdf