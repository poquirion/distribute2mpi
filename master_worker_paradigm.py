#!/usr/bin/env python
from mpi4py import MPI
from random import random
from time import sleep
import sys
import socket
import argparse
import logging
import os
import queue
import uuid
import threading

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
        self.job_queue = queue.Queue()
        self.nproc = n_proc
        self.worker_pool = queue.Queue()
        self.waiting_worker = queue.deque
        self.running_worker = queue.deque
        self.running_jobs = queue.deque()
        self.waiting_jobs = queue.deque()
        self.worker_monitor_thread = None
        self.job_monitor_thread = None
        self.comm = None

    def join(self):

        while not self.job_queue.empty():
            sleep(1)



    def map_mpi(self, func, iterable):

        for args in iterable:
            self.job_queue.put_nowait({self.UID: uuid.uuid4(), self.FUNC: func, self.ARGS: args})
            self.waiting_jobs.appendleft({self.UID: uuid.uuid4(), self.FUNC: func, self.ARGS: args})

        self.start()

    def start(self):

        self.spawn_worker(self.job_queue, self.nproc)

        logging.debug('worker thread')
        self.worker_monitor_thread = threading.Thread(
            target=self.worker_loop,
            args=(self.comm, self.worker_pool),
            )
        self.worker_monitor_thread.daemon = True
        self.worker_monitor_thread.start()

        logging.debug('job thread')
        self.job_monitor_thread = threading.Thread(
            target=self.job_loop,
            args=(self.comm, self.job_queue, self.worker_pool),
            )
        self.job_monitor_thread.daemon = True
        self.job_monitor_thread.start()


    def worker_loop(self, comm, worker_pool):
        """ This loop add available worker to worker queue

        :param comm:
        :param worker_pool:
        :return:
        """
        logging.info('worker loop on')
        while True:
            new_rank = comm.recv(tag=self.TAG_AVAILABLE)
            logging.debug('adding worker {}'.format(new_rank))
            worker_pool.put_nowait(new_rank)

    def job_loop(self, comm, job_queue, worker_pool):
        logging.info('job loop on')

        while True:
            # when job is None, nothing left tp do!
            logging.debug('waiting for job  {}'.format(job_queue.qsize()))
            job = job_queue.get_nowait()

            logging.debug('waiting for worker')
            # Blocking call
            a_worker = worker_pool.get()
            logging.debug('sending {} to {}'.format(job, a_worker))
            comm.send(job, a_worker, tag=self.TAG_JOB_TO_WORKER)
            logging.debug('sent to worker {}'.format(a_worker))

    def spawn_worker(self, job_queue, n_process):
        '''Sends jobs to slaves
        :return:
        '''

        logging.info('Spawning worker form {}!'.format(socket.gethostname()))

        # TODO spawn many comm with one process each instead of many process with one comm

        self.comm = MPI.COMM_WORLD.Spawn(
            sys.executable, args=[os.path.realpath(__file__), '--mode ', 'worker'], maxprocs=n_process)

        logging.info('spawned {} mpi workers'.format(n_process))



    def exec_pool(self):
        """Execute jobs sent from master
        """
        hostname = socket.gethostname()
        try:
            self.comm = MPI.Comm.Get_parent()
            rank = self.comm.Get_rank()
        except:
            raise ValueError('Could not connect to parent - ')

        logging.info('worker {} running'.format(rank))

        # log = 'I AM {}, rank {}'.format(hostname, rank)
        # self.comm.gather(sendobj=log, root=0)
        i = 0
        while True:

            logging.info('worker {} available'.format(rank))
            self.comm.send(rank, 0, tag=self.TAG_AVAILABLE)
            # wait for things to do
            logging.debug('worker {} registered'.format(rank))

            the_job = self.comm.recv(source=0, tag=self.TAG_JOB_TO_WORKER)

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

            self.comm.send(the_job, 0, tag=self.TAG_JOB_TO_MASTER)

            i += 1

            logging.debug('rank = {} job = {} loop num = {}'.format(self.comm.rank, the_job,i))


def local_one(*args):
    hostname = socket.gethostname()
    log = 'I am on {},  and this is local_one processing these args {}'.format(hostname, args)
    logging.info(log)
    sleep(3)
    return '{}\n and these are your args\n {}'.format(log, args)


def test_mpi_pool(n_proc=1):

    pool = MpiPool(n_proc=n_proc)

    args = [(1, 2, 3), ("nous", "allons", "aux", "bois")]

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

    FORMAT = "--%(lineno)5s %(funcName)15s():  %(message)s"

    if parsed.mode == 'worker':
        # Use a file handle when more than one worker !!!
        logging.basicConfig(level=logging.INFO, filename='workers.log', format=FORMAT)
        worker = MpiPool()
        worker.exec_pool()
    else:
        logging.basicConfig(level=logging.DEBUG, format=FORMAT)
        test_mpi_pool(n_proc=parsed.np)


if __name__ == '__main__':
    main()

