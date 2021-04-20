#!python

import sys
import re
from time import mktime, strptime, strftime


class DbuParser:

    log4j_time_format = "%y/%m/%d %H:%M:%S"
    nice_time_format = "%a, %d %b %Y %H:%M:%S"

    @staticmethod
    def parse_time(timestr: str):
        return strptime(timestr, DbuParser.log4j_time_format)

    @staticmethod
    def parse_ts(timestr: str):
        return mktime(strptime(timestr, DbuParser.log4j_time_format))

    def __init__(self):

        self.re_parser_time = re.compile(r"^(\S+ \S+) INFO ")
        self.re_parser = re.compile(r"^(\S+ \S+) INFO .+ Executor updated: .+ is now (\S+)")

        self.started_at = 0

        self.current_executors = 0
        self.max_executors = 0

        self.previous_checkpoint_ts = 0
        self.previous_line_time = None

        self.integral_seconds = 0  # total worker-seconds
        self.stopped_at = self.total_runtime = self.avg_executors = None

    def match_process(self, when_ts, what):

        self.integral_seconds += (when_ts - self.previous_checkpoint_ts) * self.current_executors

        if what == 'RUNNING':
            self.current_executors += 1
            if self.max_executors < self.current_executors:
                self.max_executors = self.current_executors
        elif what == 'LOST':
            self.current_executors -= 1
            assert self.current_executors >= 0, "Number of active executors can't be negative"
        elif what == 'END':
            self.current_executors = 0

        self.previous_checkpoint_ts = when_ts

        # print(f"{when_ts}: {self.current_executors} active executors; "
        #       f"accumulated {self.integral_seconds / 60:.01f} executor-minutes")

    def first_line(self, line):
        self.previous_line_time = DbuParser.parse_time(line[0:17])
        self.started_at = mktime(self.previous_line_time)
        print(f"""Job started at {strftime(self.nice_time_format, self.previous_line_time)}""")

    def print_graph(self, line):
        if not self.re_parser_time.match(line):
            return
        current_time = DbuParser.parse_time(line[0:17])
        if current_time.tm_min != self.previous_line_time.tm_min:
            print(f"""{strftime("%H:%M", current_time)} {'*' * self.current_executors}""")
            self.previous_line_time = current_time

    def try_match(self, line):
        match = self.re_parser.match(line)
        if not match:
            return

        (when, what) = match.groups()  # ts, running/lost
        when_ts = DbuParser.parse_ts(when)

        self.match_process(when_ts, what)

    def finalize(self, line):
        self.stopped_at = DbuParser.parse_ts(line[0:17])
        self.total_runtime = int(self.stopped_at - self.started_at)
        print(f"""Job finished at {strftime(self.nice_time_format, DbuParser.parse_time(line[0:17]))}""")

        self.match_process(self.stopped_at, 'END')

        print(f"Script runtime {int(self.total_runtime / 60)}m {self.total_runtime % 60}s,"
              f" or {self.total_runtime / 60:.01f} driver-minutes")

        self.avg_executors = self.integral_seconds / self.total_runtime
        print(f"Max.executors: {self.max_executors}; Avg.executors: {self.avg_executors:.01f}; "
              f"total {self.integral_seconds / 60:.01f} worker-minutes")


def main(filename: str, print_executors_graph: bool = True):

    print(f"Processing {filename}")

    linecount = 0

    parser = DbuParser()

    with open(filename, "r") as f:
        for line in f:
            if not linecount:
                parser.first_line(line)

            linecount += 1

            if print_executors_graph:
                # optional graph - number of executors
                parser.print_graph(line)

            parser.try_match(line)

    print(f"{linecount:,} lines processed.")

    parser.finalize(line)


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print(f"{sys.argv[0]} <input_log4j.txt>")
        sys.exit(2)

    main(sys.argv[1])
