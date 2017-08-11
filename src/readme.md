## List of libraries to install:

**argparse**        - `pip install argparse`
**pytz**            - `pip install pytz`
**lzma**            - `pip install pyliblzma`
**Multiprocessing** - `pip install multiprocess`
**Progressbar**     - `pip install progressbar2`

## List of available options
### positional arguments:
* directory
Logfiles directory

### optional arguments:
* `-l`, `--list_vm_host`
Print all VMs and hosts in given time range (or without it in all log)

* `-f` FILENAMES [FILENAMES ...], `--filenames` FILENAMES [FILENAMES ...]
List of logfiles filenames (with expansion)

* `--default_tzinfo` DEFAULT_TZINFO [DEFAULT_TZINFO ...]
Specify time zones for all files (will be used) if file datetime does not have time zone (example: --default_tzinfo -0400). If not specified - UTC is used

* `--tzinfo` TZINFO [TZINFO ...]
Specify time zones for files (will be used) if file datetime does not have tz (example: --tzinfo engine -0400 vdsm +0100). Default time zone: UTC or set with --default_tzinfo

* `-p` PRINT, `--print` PRINT
Where to print the output (filename, "stdout" or "stderr")

* `-o` OUT, `--out` OUT     Directs the output to the file

* `-d` OUTPUT_DIR, `--output_dir` OUTPUT_DIR
Specify directory to save program output

* `--format_file` FORMAT_FILE
Filename with formats of log files (with path and expansion). Default: "format_templates.txt"

* `-t` TIME_RANGE [TIME_RANGE ...], `--time_range` TIME_RANGE [TIME_RANGE ...]
Specify time range(s) (in UTC) for analysis. Type even number of space-separated times (1st for time range beginning and 2nd for ending) in the following format (example) 2000-01-31T21:10:00,123 2000-01-31T22:10:00,123

* `--vm` VM [VM ...]
Specify VM id(s) to find information about

* `--host` HOST [HOST ...]
Specify host id(s) to find information about

* `--event` EVENT [EVENT ...]
Specify event(s) to find information about (raw text of event, part of message or a key word), use quotes for messages with spaces (example: --event warning "down with error" failure)

* `-w`, `--warn`
Print parser warnings about different log lines format

* `--progressbar`
Show full-screen progress bar for parsing process
