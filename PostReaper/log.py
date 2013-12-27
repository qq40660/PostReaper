import time
import threading


def get_time():
    return time.strftime(u"%Y-%m-%d %A %X %Z", time.localtime(time.time()))


class Logger:
    def __init__(self, log_file):
        self._fd = open(log_file, u"a")

    def debug(self, msg):
        msg = u"".join([str(threading.current_thread()), u" | DEBUG | ",
                       msg, u" | ", get_time(), "\n"])
        self._fd.write(msg)

    def error(self, msg):
        msg = u"".join([str(threading.current_thread()), u" | ERROR | ",
                       msg, u" | ", get_time(), "\n"])
        self._fd.write(msg)
