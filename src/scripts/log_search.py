import sys
from lib import log_parser
from time import strptime


def parse_arguments(arguments):
    vm_name = arguments[1]
    start_time = strptime(arguments[2] + ' ' + arguments[3], '%Y-%m-%d %H:%M:%S')
    end_time = strptime(arguments[4] + ' ' + arguments[5], '%Y-%m-%d %H:%M:%S')
    file_name = arguments[6]
    return vm_name, start_time, end_time, file_name


def main(arguments):
    kwargs = parse_arguments(arguments)
    s = log_parser.LogParser(*kwargs)
    s.search()


if __name__ == '__main__':
    main(sys.argv)