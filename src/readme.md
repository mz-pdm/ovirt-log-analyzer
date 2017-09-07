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

* `*_frequent.txt` - messages that were removed from the output by "Exclude frequent messages" criteria

* If -o flag, the result will be saved to the file (to stdout otherwise)

* `log_analyzer_cache` folder within the provided logfiles folder contain information about found VMs, hosts, tasks, and symbol positions for given time ranges
