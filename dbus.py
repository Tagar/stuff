#!python

import sys
import re
from time import mktime, strptime, strftime

log4j_time_format = "%y/%m/%d %H:%M:%S"
nice_time_format = "%a, %d %b %Y %H:%M:%S"


def parse_time(timestr: str):
    return strptime(timestr, log4j_time_format)


def parse_ts(timestr: str):
    return mktime(strptime(timestr, log4j_time_format))


def main(filename: str, print_executors_graph: bool = True):
    
    print(f"Processing {filename}")

    parser_time = re.compile(r"^(\S+ \S+) INFO ")
    parser = re.compile(r"^(\S+ \S+) INFO .+ Executor updated: .+ is now (\S+)")

    linecount = 0
    started_at = 0

    current_executors = 0
    max_executors = 0

    previous_checkpoint_ts = 0
    previous_line_time = None

    integral_seconds = 0  # total worker-seconds

    with open(filename, "r") as f:
        for line in f:
            if not linecount:
                previous_line_time = parse_time(line[0:17])
                started_at = mktime(previous_line_time)
                print(f"""Job started at {strftime(nice_time_format, previous_line_time)}""")

            linecount += 1

            if print_executors_graph and parser_time.match(line):
                # optional graph - number of executors
                current_time = parse_time(line[0:17])
                if current_time.tm_min != previous_line_time.tm_min:
                    print(f"""{strftime("%H:%M", current_time)} {'*'*current_executors}""")
                    previous_line_time = current_time

            match = parser.match(line)
            if not match:
                continue

            (when, what) = match.groups()  # ts, running/lost
            when_ts = parse_ts(when)

            integral_seconds += (when_ts - previous_checkpoint_ts) * current_executors

            if what == 'RUNNING':
                current_executors += 1
                if max_executors < current_executors:
                    max_executors = current_executors
            elif what == 'LOST':
                current_executors -= 1
                assert current_executors >= 0, "Number of active executors can't be negative"

            previous_checkpoint_ts = when_ts

            # print(f"{when}: {current_executors} active executors; "
            #       f"accumulated {integral_seconds/60:.01f} executor-minutes")

    print(f"{linecount:,} lines processed.")

    stopped_at = parse_ts(line[0:17])
    total_runtime = int(stopped_at - started_at)
    print(f"""Job finished at {strftime(nice_time_format, parse_time(line[0:17]))}""")

    print(f"Script runtime {int(total_runtime / 60)}m {total_runtime % 60}s,"
          f" or {total_runtime / 60:.01f} driver-minutes")

    avg_executors = integral_seconds / total_runtime
    print(f"Max.executors: {max_executors}; Avg.executors: {avg_executors:.01f}; "
          f"total {integral_seconds / 60:.01f} worker-minutes")


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print(f"{sys.argv[0]} <input_log4j.txt>")
        sys.exit(2)

    main(sys.argv[1])
