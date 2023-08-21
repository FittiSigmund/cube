import time
from typing import Dict


class PythonTimer(object):
    _instance = None
    start_time: int | None = None
    stop_time: int | None = None
    times: Dict[int, int] = {}

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(PythonTimer, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        if not self.start_time:
            self.start_time = time.perf_counter_ns()
        else:
            raise Exception("PythonTimer is running. Call stop first.")

    def stop(self):
        if not self.start_time:
            raise Exception("Start PythonTimer first")
        else:
            self.stop_time = time.perf_counter_ns()
            self.times[self.start_time] = self.stop_time
            self.start_time = None
            self.stop_time = None

    def elapsed_time(self):
        if self.times and self.start_time is None:
            return sum(map(lambda x: x[1] - x[0], self.times.items()))
        else:
            raise Exception("Times dict doesn't exist or PythonTimer is running")


class DBTimer(object):
    _instance = None
    start_time: int | None = None
    stop_time: int | None = None
    times: Dict[int, int] = {}

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DBTimer, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        if not self.start_time:
            self.start_time = time.perf_counter_ns()
        else:
            raise Exception("DBTimer is running. Call stop first.")

    def stop(self):
        if not self.start_time:
            raise Exception("Start DBTimer first")
        else:
            self.stop_time = time.perf_counter_ns()
            self.times[self.start_time] = self.stop_time
            self.start_time = None
            self.stop_time = None

    def elapsed_time(self):
        if self.times and self.start_time is None:
            return sum(map(lambda x: x[1] - x[0], self.times.items()))
        else:
            raise Exception("Times dict doesn't exist or DBTimer is running")
