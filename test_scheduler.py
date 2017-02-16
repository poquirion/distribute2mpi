import logging
import unittest.mock as mock
import os
import shutil
import tempfile
import unittest
from unittest import TestCase

import schedule

logging.basicConfig(level=logging.INFO)


class TestJob(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix='schedule_test')
        self.log = 'truite.log'
        self.log_path = '{}'.format(os.path.join(self.directory, self.log))
        self.echo_vall = '33'
        self.cmd_line = 'echo {}'.format(self.echo_vall, self.log_path)
        logging.info(self.cmd_line)
        self.args = self.cmd_line.split()
        self.to_submit_job = None

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def test_local(self):
        with mock.patch('schedule.Job') as mock_Job:
            mock_Job._local_submit.return_value = 0
            job = mock_Job(self.args, log_path=self.log_path)
            p = job.submit()
            self.assertEquals(p, 0)

    def test_already_running(self):
        pass


class TestJobList(TestCase):
    pass



