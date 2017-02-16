"""
This module provide a poor man job scheduler with dynamic status and log access
"""
import json
import os
import queue
import subprocess
import tempfile
import unittest
import uuid
import shutil
import logging


class baseJob(object):
    """
    This is the base class to be store, query, executed by agent, killed, ...
    """
    _local = 'local'
    _qsub = 'qsub'
    _briaree = 'briaree'
    _guillimin = 'guillimin'
    _modes = {_local: '',
              _qsub: 'qsub',
              _briaree: 'qsub -A briaree options',
              _guillimin: 'qsub guillimin options'}

    def __init__(self, path):
        pass
    def run(self):
        pass

    def all_log(self):
        pass

    def new_log(self):
        pass

    def kill(self):
        pass

    @classmethod
    def monitor_factory(cls, path):
        '''

        :param path: path to a job log
        :return: An job object initialized from path info
        '''

        return cls(path)


class SubProcessJob(baseJob):
    """
    Run a arbtrary subprocess
    """



    def __init__(self, path=None, cmd=None, args=None, mode=None, cwd=None,
                 env=None, name=None, log_path = None, uid=None):


        if path:
            pass

        if mode in self._modes:
            self.mode = self._modes[mode]
        else:
            self.mode = self._modes[self._local]

        self.args = args
        self.cwd = cwd
        self.env = env
        self.hostname = None
        if uid is None:
            self.uid = uuid.uuid4()
        else:
            self.uid = uid

        if name is None:
            self.name = self.uid
        else:
            self.name = name

        self.log_path = log_path
        self._process = None



    def run(self):

        # stdout = stderr = open(self.log_path, 'w')
        self._process = subprocess.Popen(self.args, env=self.env, cwd=self.cwd)
        return self._process.wait()




class pythonJob(baseJob):
    """
    Dynamically import the python module and run an arbitrary method from it.
    """

    def run(self):
        pass




class agent(object):
    _file = 'file'
    _modes = [_file]

    job_queue = queue.Queue()



    def __init__(self, mode=_file, job_file=None):

        self.job_file = job_file
        if mode in self._modes:
            self.mode = mode
        else:
            self.mode = self._file

        self.interrupt = False


    @property
    def next_job(self):
        return

    def init_queue(self, job_file):
        with open(job_file) as fp:
            json.load(fp)

    def start(self):
        if self.mode == self._file:
            self.init_queue(self.job_file)

        while not self.interrupt:

            self.run(self.next_job)

    def run(self, job):
        pass






class JobList(list):
    def __init__(self, args, path=None):

        super(JobList, self).__init__(args)






if __name__ == '__main__':
    j = Job(cmd = 'echo')