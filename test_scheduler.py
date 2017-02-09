import logging
import os
import shutil
import tempfile
import unittest

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
        shutil.rmtree(self.directory, ignore_errors=True )

    def _test_local(self):
        job = self.to_submit_job = schedule.Job(self.args, log_path=self.log_path)
        p = job.submit()
        p.wait()
        with open(self.log_path) as fp:
            self.assertEquals(fp.readline()[:-1], self.echo_vall)

    def test_already_running(self):
        pass
