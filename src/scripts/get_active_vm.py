import sys
sys.path.append("../lib")
import log_parser

s = log_parser.LogParser()
print(s.get_all_vm_active_by_time())
