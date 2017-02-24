#!/usr/bin/env python
from mpi4py import MPI
import  random
from time import sleep
import sys
import socket
import argparse
import logging
import os
import queue
import asyncio
import uuid
import threading
import psutil


#TODO Build process queue with status
#TODO Build Job Queue(s) with status


test_dir = "/home/poq/mpi4py_test"
# comm = MPI.COMM_WORLD
# mode = MPI.MODE_WRONLY|MPI.MODE_CREATE#|MPI.MODE_APPEND


# job_queue = [{'func':method, 'id': job_uid, 'args', args}]

class MpiPool(object):
    """Emulate multiprocess poll but with an mpi interface
    """
    UID = 'uid'
    FUNC = 'func'
    ARGS = 'args'
    RET_VAL = 'return'
    STATUS = 'status'
    FAILED = 'failed'
    DONE = 'done'
    WAITING = 'waiting'
    ALL_STATUS = [FAILED, DONE, WAITING]

    TAG_AVAILABLE = 1
    TAG_JOB_TO_WORKER = 2
    TAG_JOB_TO_MASTER = 3



    def __init__(self, n_proc=None):
        """
        :param n_proc: If None, probably an exec loop
                    (Could start synchrone job processing if > 1 will run asynchrone)

        """
        # self.job_queue = queue.Queue()
        self.job_queue = queue.Queue()
        self.nproc = n_proc
        self.worker_pool = queue.Queue()
        # self.worker_pool = asyncio.Queue()
        self.waiting_worker = queue.deque()
        self.running_worker = queue.deque()
        self.running_jobs = queue.deque()
        self.completed_jobs = queue.Queue()
        self.worker_monitor_thread = None
        self.job_monitor_thread = None
        self.icomm = None
        self._is_close = False

    def close(self):
        self._is_close = True

    def join(self):

        while not self.job_queue.empty():
            sleep(1)

    def map_mpi(self, func, iterable):

        for args in iterable:
            self.job_queue.put_nowait({self.UID: uuid.uuid4(), self.FUNC: func, self.ARGS: args})

        self.start()

    def start(self):

        self.spawn_worker(n_process=self.nproc)

        logging.debug('launch add worker')
        self.worker_monitor_thread = threading.Thread(
            target=self.__add_worker_to_pool,
            args=(self.icomm, self.worker_pool))
        self.worker_monitor_thread.start()

        logging.debug('launch receive results')
        self.completed_jobs_thread = threading.Thread(
            target=self.__receive_results,
            args=(self.icomm, self.completed_jobs))
        self.completed_jobs_thread.start()

        logging.debug('launch send job')
        self.job_monitor_thread = threading.Thread(
            target=self.__send_job,
            args=(self.icomm, self.job_queue, self.worker_pool))
        self.job_monitor_thread.start()

        # logging.debug('launch send job')
        # self.debug_thread = threading.Thread(
        #     target=self.debug,
        #     args=(self.icomm, self.job_queue, self.worker_pool))
        # self.debug_thread.start()

    # def debug(self, icomm, jq, wp):
    #
    #     while True:
    #         logging.info('HELP')
    #         sleep(.2)

    def __receive_results(self, icomm, completed_jobs):

        logging.info('receive loop on')
        done_job = None
        while True:
            # blocking call
            logging.debug('waiting for done job')
            done_job = None
            # while done_job is None:
            # done_job = icomm.recv(source=MPI.ANY_SOURCE, tag=self.TAG_JOB_TO_MASTER)
            done_job = self.icomm.recv(source=MPI.ANY_SOURCE, tag=self.TAG_JOB_TO_MASTER)
            # logging.info('')
            logging.debug('adding done job {}'.format(done_job))
            completed_jobs.put_nowait(done_job)

        logging.info('receive loop off')

    def __add_worker_to_pool(self, icomm, worker_pool):
        """ This loop add available worker to worker queue

        :param icomm:
        :param worker_pool:
        :return:
        """
        logging.info('worker loop on')
        while True:
            # blocking call
            logging.debug('waiting for new worker')
            new_rank = None
            # while new_rank is None:
            # new_rank = comm.recv(source=MPI.ANY_SOURCE, tag=self.TAG_AVAILABLE)
            new_rank = self.icomm.recv(source=MPI.ANY_SOURCE, tag=self.TAG_AVAILABLE)
            logging.debug('adding worker {}'.format(new_rank))
            worker_pool.put_nowait(new_rank)
        logging.info('worker loop off')

    def __send_job(self, icomm, job_queue, worker_pool):
        logging.info('job loop on')
        while True:
            # when job is None, nothing left tp do!
            logging.debug('waiting for job  {}'.format(job_queue.qsize()))

            job = None
            while job is None:
                try:
                    job = job_queue.get(timeout=1)
                except queue.Empty:
                    if self._is_close:
                        # all jobs are done, yé!
                        self.soft_terminate()

            logging.debug('Got job job  {}'.format((job[self.FUNC], job[self.ARGS])))

            logging.debug('waiting for worker')
            # Blocking loop
            while worker_pool.empty():
                sleep(1)
            a_worker = worker_pool.get()

            logging.debug('sending {} to {}'.format((job[self.FUNC], job[self.ARGS]), a_worker))
            icomm.send(job, dest=a_worker, tag=self.TAG_JOB_TO_WORKER)
            # _ = icomm.recv(source=MPI.ANY_SOURCE, tag=33)
            # logging.info('got it {}'.format(_))
            logging.debug('sent to worker {}'.format(a_worker))
            # break
        logging.info('job loop off')

    def soft_terminate(self, timeout=None):
        """Waite for the last process to finish or timeout to be reached
        before calling terminate
        :param timeout:
        :return:
        """

    def terminate(self):
        """Send kill signal to workers and child process

        :return:
        """
        pass

    def spawn_worker(self, n_process):
        '''Sends jobs to slaves
        :return:
        '''

        logging.info('Spawning worker form {}!'.format(socket.gethostname()))

        # TODO spawn many comm with one process each instead of many process with one comm

        self.icomm = MPI.COMM_WORLD.Spawn(
            sys.executable, args=[os.path.realpath(__file__), '--mode ', 'worker'], maxprocs=n_process)

        logging.info('spawned {} mpi workers'.format(n_process))

class MPIWorker(MpiPool):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def exec_pool(self):
        """Execute jobs sent from master
        Will exit if none is received.
        """
        hostname = socket.gethostname()
        try:
            self.icomm = MPI.Comm.Get_parent()
            rank = self.icomm.Get_rank()
        except:
            raise ValueError('Could not connect to parent - ')

        logging.info('worker {} running'.format(rank))

        # log = 'I AM {}, rank {}'.format(hostname, rank)
        # self.comm.gather(sendobj=log, root=0)
        i = 0
        while True:

            logging.info('worker {} available'.format(rank))
            self.icomm.send(rank, dest=0, tag=self.TAG_AVAILABLE)
            # wait for things to do

            logging.debug('worker {} waiting for new job'.format(rank))
            # blocking
            the_job = self.icomm.recv(source=0, tag=self.TAG_JOB_TO_WORKER)
            # self.icomm.send(33, dest=0, tag=33)

            logging.debug('worker {} got job\n{}'.format(rank, the_job))

            if the_job is None:
                # No more jobs to do
                break

            function = the_job[self.FUNC]
            args = the_job[self.ARGS]

            try :
                fct_ret_val = function(*args)
                status = self.DONE
            except Exception as e:
                fct_ret_val = None
                status = self.FAILED
            the_job[self.RET_VAL] = fct_ret_val
            the_job[self.STATUS] = status

            logging.info('{} finish with status {}'.format(the_job[self.FUNC], status))
            logging.debug('sending results back to master')
            self.icomm.send(the_job, 0, tag=self.TAG_JOB_TO_MASTER)

            i += 1
            logging.debug('rank = {} job = {} loop num = {}'.format(self.icomm.rank, (the_job[self.FUNC], the_job[self.ARGS]), i))

        logging.info('worker {} on {} terminating'.format(rank,hostname))

    def __auto_kill(self):
        """Kill current process and its children

        :return:
        """
        pid = os.getpid()
        me = psutil.Process(pid)
        for proc in me.children(recursive=True):
            proc.kill()
        # auto destroy
        me.kill()

def local_one(*args):
    hostname = socket.gethostname()
    log = 'I am on {},  and this is local_one processing these args {}'.format(hostname, args)
    logging.info(log)
    sleep(3)
    return '{}\n and these are your args\n {}'.format(log, args)


def test_mpi_pool(n_proc=1):

    pool = MpiPool(n_proc=n_proc)
    a = [[j for j in range(random.randint(1, 5))] for i in range(3)]
    args = [(1, 2, 3), ("nous", "allons", "aux", "bois"), *a]

    pool.map_mpi(local_one, args)

    # blocking statement
    # TODO put a blocking statement that can Show a progress status with the job and workers
    pool.join()


def main(args=None):

    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(description='A Master Worker MPI setup')
    parser.add_argument("--mode", default='master')
    parser.add_argument("-n", "--np", default=1, type=int)
    parsed = parser.parse_args(args)

    FORMAT = "%(levelname)7s --%(lineno)5s %(funcName)15s():  %(message)s"

    if parsed.mode == 'worker':
        # Use a file handle when more than one worker !!!
        logging.basicConfig(level=logging.DEBUG, filename='workers.log', format=FORMAT)
        worker = MPIWorker()
        worker.exec_pool()
    else:
        # logging.basicConfig(level=logging.DEBUG, filename='master.log', format=FORMAT)
        logging.basicConfig(level=logging.DEBUG, format=FORMAT)
        test_mpi_pool(n_proc=parsed.np)


if __name__ == '__main__':
    main()

