"""
This module provide a poor man job scheduler with dynamic status and log access
"""

import os
import subprocess
import tempfile
import unittest
import uuid
import shutil
import logging


class Job(object):
    """
    This is inspired by subprocess popen
    """

    _local = 'local'
    _qsub = 'qsub'
    _briaree = 'briaree'
    _guillimin = 'guillimin'
    _modes = {_local: '',
              _qsub: 'qsub',
              _briaree: 'qsub -A briaree options',
              _guillimin: 'qsub guillimin options'}


    def __init__(self, args, mode=None, cwd=None, env=None, name=None, log_path = None):
        if mode is None:
            self.mode = self._local
        else:
            self.mode = self._modes[mode]

        self.args = args
        self.cwd = cwd
        self.env = env
        self.hostname = None
        self.id = uuid.uuid4()
        if name is None:
            self.name = self.id
        else:
            self.name = name

        self.log_path = log_path
        self._process = None

    def submit(self):
        if self.mode is self._local:
            return self._submit_local()


    def _submit_local(self):

        stdout = stderr = open(self.log_path, 'w')
        self._process = subprocess.Popen(self.args, env=self.env, cwd=self.cwd, stdout=stdout, stderr=stderr)
        self._job_id = self._process.pid
        return self._process

    def all_log(self):
        pass
    def new_log(self):
        pass
    def kill(self):
        pass

    @property
    def job_id(self):
        return self._job_id

    @classmethod
    def submitted_job(cls, path):
        '''

        :param path: path to a job log
        :return: An job object initialized from path info
        '''

        return cls(args=None)




# if __name__ == '__main__':
#      logger = logging.basicConfig(level=logging.DEBUG)
#      unittest.main()