import psutil
import os

from time import time,sleep
from lib.logger import Logger


class ProcWrapper:
    def __init__(self, args, id, logger_name, sleep_timeout=80):
        self.__args = args
        self.__id = id
        self.__name = logger_name
        self.__sleep_time = 0
        self.__last_sleep_time = 0
        self.__sleep_timeout = sleep_timeout
        self.proc_logger = Logger(name=self.__name, color=True)

    def run(self, shell=True):
        self.__process = psutil.Popen(self.__args, shell=shell)
        self.proc_logger.log("%s proc start with pid %s" % (self.__name, self.__process.pid))

    @property
    def is_run(self):
        status = self.__process.status()
        if status == psutil.STATUS_SLEEPING:
            if self.__last_sleep_time:
                self.__sleep_time += time() - self.__last_sleep_time
            self.__last_sleep_time = time()
            # self.proc_logger.log_console("Sleep time: %s" % self.__sleep_time, status='!')
        else:
            self.__last_sleep_time = 0
            self.__sleep_time = 0
        # if self.__name == "Consumer" and status != psutil.STATUS_SLEEPING:
        #    self.proc_logger.log_console("Status %s" % status, status='!')
        if status == psutil.STATUS_STOPPED or status == psutil.STATUS_DEAD or status == psutil.STATUS_ZOMBIE or \
                        self.__sleep_time > self.__sleep_timeout:
            return False
        return True

    def stop(self):
        try:
            self.__process.terminate()
        except OSError:
            pass


class RunManager:
    def __init__(self, consumer_sh=None, producer_sh=None):
        self.run_logger = Logger(name="RunManager", logfile="data/logs/runmanager.log")
        self.__producers_list = []
        self.__consumers_list = []
        self.__consumer_sh_path = consumer_sh
        self.__producer_sh_path = producer_sh

    def start(self, producers_count, consumers_count, from_systemd=False):
        self.run_logger.log("Start producers. Count of producers %s" % producers_count)
        if from_systemd:
            fd = open("data/systemd/consumer.service")
            consumer_data = fd.read()
            fd.close()
            fd = open("data/systemd/producer.service")
            producer_data = fd.read()
            fd.close()

            for index in range(producers_count):
                fd = open("/etc/systemd/system/producer%s.service" % (index + 1), "w")
                fd.write(producer_data)
                fd.close()
                os.system("systemctl start producer%s.service" % (index + 1))
            for index in range(consumers_count):
                fd = open("/etc/systemd/system/consumer%s.service" % (index + 1), "w")
                fd.write(consumer_data)
                fd.close()
                os.system("systemctl start consumer%s.service" % (index + 1))
            return

        if self.__consumer_sh_path is None or self.__producer_sh_path is None:
            self.run_logger.log_console("Shell script does not set", status='~')
            return

        for index in range(producers_count):
            pr = ProcWrapper([self.__producer_sh_path], index, "Producer")
            pr.run(shell=False)
            self.__producers_list.append(pr)

        for index in range(consumers_count):
            pr = ProcWrapper([self.__consumer_sh_path], index, "Consumer")
            pr.run(shell=True)
            self.__consumers_list.append(pr)

        while True:
            delete_list = []
            for proc in self.__consumers_list:
                if not proc.is_run:
                    proc.proc_logger.log("Proc end")
                    proc.stop()
                    delete_list.append(proc)

            for proc in delete_list:
                self.__consumers_list.remove(proc)

            to_run = consumers_count - len(self.__consumers_list)
            for index in range(to_run):
                    pr = ProcWrapper([self.__consumer_sh_path], index, "Consumer")
                    pr.run(shell=True)
                    self.__consumers_list.append(pr)

    def stop(self, producers_count, consumers_count, from_systemd=True):
        self.run_logger.log("Stop producers. Count of producers %s" % producers_count)
        if from_systemd:
            for index in range(producers_count):
                os.system("systemctl stop producer%s.service" % (index + 1))

            for index in range(consumers_count):
                os.system("systemctl stop consumer%s.service" % (index + 1))

    def delete(self, producers_count, consumers_count, from_systemd=True):
        self.run_logger.log("Delete producers and counters")
        if from_systemd:
            for index in range(producers_count):
                os.system("rm /etc/systemd/system/producer%s.service" % (index + 1))

            for index in range(consumers_count):
                os.system("rm /etc/systemd/system/consumer%s.service" % (index + 1))
