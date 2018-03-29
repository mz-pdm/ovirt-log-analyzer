# ovirt-log-analyzer

[oVirt](http://www.ovirt.org) together with the underlying software (such as
[libvirt](http://www.libvirt.org) and [QEMU](http://www.qemu-project.org))
produces a lot of different logs.  It's not always easy to find a particular
piece of information or to gather all important events of a virtual machine
life cycle in all the log data.

ovirt-log-analyzer is a tool for sorting, correlating, filtering, marking, and
highlighting the log data and presenting them in a comprehensible form to the
user.

## List of libraries to install

ovirt-log-analyzer requires Python 3 and the following Python libraries:

* **argparse**        - `pip install argparse`
* **pytz**            - `pip install pytz`
* **lzma**            - `pip install pyliblzma`
* **Multiprocessing** - `pip install multiprocess`
* **Progressbar**     - `pip install progressbar2`
* **NumPy**           - `pip install numpy`

## Basic usage

Assuming your oVirt logs are stored in DIRECTORY, you run the analyzer as

    python …/ovirt-log-analyzer/src/analyze_logs.py DIRECTORY

This produces some files in the current directory, most notably result.txt
file.  That file contains the basic results of the log analysis.

See next sections for details.

## List of available options

### positional arguments:

* DIRECTORY - directory with the log files

### optional arguments:
* `-l`, `--list_vm_host`
Print all VMs and hosts in given time range (or without it in all log)

* `-f` FILENAMES [FILENAMES ...], `--filenames` FILENAMES [FILENAMES ...]
Log files to process; use `all` to process all log files; if not given then common oVirt log files are processed

* `--default_tzinfo` DEFAULT_TZINFO [DEFAULT_TZINFO ...]
Specify time zones for all files (will be used) if file datetime does not have time zone (example: --default_tzinfo -0400). If not specified - UTC is used

* `--tzinfo` TZINFO [TZINFO ...]
Specify time zones for files (will be used) if file datetime does not have tz (example: --tzinfo engine -0400 vdsm +0100). Default time zone: UTC or set with --default_tzinfo

* `-p` PRINT, `--print` PRINT
Where to print the output (filename, "stdout" or "stderr")

* `-o` OUT, `--out` OUT
File to output results to; result.txt by default; use `-' for standard output

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

* `--additive`
Search for messages that contain user-defined VMs OR hosts

* `--criterias`
Criterias of adding a message to the output. Available:
	- `Subtasks` - Show messages containing information about VM tasks and subtasks
	- `Error or warning` - Show messages with errors of warnings
	- `Differ by VM ID` - Show messages that appear with several different VMs
	- `Exclude frequent messages` - Remove frequent messages from the output (find them in "_frequent.txt")
	- `Increased errors` - Show messages that are followed by increasing number of errors or warnings
	- `Long operations` - Show messages containing information about long operations (with time of its execution)
	- (Not included to the flag variables) `Event=*`, `VM=*`, `Host=*` - indicates the corresponding entity found in this message
	- (Not included to the flag variables) `Unique` - Marks messages that were alone inside the cluster (unique combination of the first two words)
	- (Not included to the flag variables) `Rare` - Marks messages that are in the cluster with smaller size (fewer messages) (on 3 standard deviations from mean)
	- (Not included to the flag variables) `Many messages` - Marks messages from big clusters (with large number of similar messages) (on 3 standard deviations from mean). These messages appear in _frequent file

Default is all.

* `--reload`
Reload cached VMs, hosts, tasks and logfile positions for the new given time range.

* `--clear`
Remove all cached files (log_analyzer_cache)

## Output
Log analyzed produce several output files. You can place them into a directory by using -d flag. These files are:

* `*_VMs_timeline.json` - information about time of VMs running on hosts and migrating between hosts

* `*_clusters.txt` - groups of similar messages that were analyzed (affects filtering by frequency)

* `*_commands.json` - full list of found tasks with time of execution (if both "start" and "finish" were detected)

* `*_commands_by_id.json` - reconstructed hierarchy of tasks (may contain only a few of tasks from the _commands file)

* `frequent.txt` - messages that were removed from the output by "Exclude frequent messages" criteria

* `result.txt` - results of the analysis, use `-o' command line option to use a different file name or standard output

* `log_analyzer_cache` folder within the provided logfiles folder contain information about found VMs, hosts, tasks, and symbol positions for given time ranges

## Emacs UI

The result file produced by ovirt-log-analyzer can be viewed as is or it can be
handled in an Emacs UI providing some useful functionality over it.

The easiest way to view the file in the Emacs UI is by running

    emacs -l …/ovirt-log-analyzer/emacs/ovirt-log-analyzer.el RESULT-FILE -f ovirt-log-analyzer-mode

Alternatively, you can add the mode to your running Emacs with
`M-x load-file RET …/ovirt-log-analyzer/emacs/ovirt-log-analyzer.el RET`.
Then you can open a result file created by ovirt-log-analyzer using `C-x C-f`
command and enable the UI by typing `M-x ovirt-log-analyzer-mode RET`.

key        | command
-----------|------------
tab        | move to next interesting point
backtab    | move to previous interesting point
M-n        | move to next log line
M-p        | move to previous log line
return     | jump to log
M-return   | jump to frequent.txt file
f          | filter lines
a          | toggle filter
t          | show line tags
T          | toggle line truncation
