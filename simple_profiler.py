"""Allows light-weight profiling of code execution."""

import time


class Profiler:
    """Collects messages with timestamps so you can profile your code."""
    
    def __init__(self):
        self.clear()

    def add_event(self, message):
        milliseconds = int(round(time.time() * 1000))
        self._profile_events.append((message[0:30], milliseconds))

    def clear(self):
        self._profile_events = []

    def __str__(self):
        return self._get_profile()

    def _get_profile(self):
        output = [
            "",
            "Message                        Run Time  Total time",
            "---------------------------------------------------",
        ]
        rows = []
        i = 0
        previous_time = None
        net_time = 0
        for message, time in self._profile_events:
            if i is not 0:
                t = time - previous_time
                net_time += t
                rows[i - 1][1] = t
            previous_time = time
            rows.append([message, 0, net_time])
            i += 1
        for row in rows:
            output.append('%-30s %-8s  %10s' % (row[0], row[1], row[2]))
        return "\n".join(output)
