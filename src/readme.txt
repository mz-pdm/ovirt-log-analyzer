Command to run:

python summarize_logs.py ovirtlogs engine vdsm-1 vdsm-2 -odir result -o out.txt -err stderr -js -d graph

To plot error graph via graphviz:
dot -Tpdf result/graph.dot -o result/graph.pdf