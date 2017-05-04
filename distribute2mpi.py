#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import random
import socket
import sys
import threading
import uuid
from time import sleep

import psutil
from mpi4py import MPI

try :
    import dill
    MPI.pickle.dumps = dill.dumps
    MPI.pickle.loads = dill.loads
except ImportError:
    logging.warning('dill not installed')

try:
    import queue
except ImportError:
    import Queue as queue

__version__ = "0.2.6"

#TODO Build process queue with status
#TODO Build Job Queue(s) with status


class Job(object):

    FAILED = 'failed'
    DONE = 'done'
    WAITING = 'waiting'
    WAITING = 'ready'
    ALL_STATUS = [FAILED, DONE, WAITING]

    def __init__(self, func, args, uid=None, status=None):

        if uid == None:
            self.uid = uuid.uuid4()
        self.args = args
        if status is None:
            self.status = self.WAITING
        elif status in self.ALL_STATUS:
            self.status = status
        else:
            m = 'status {} not recognised'.format(status)
            raise IOError(m)
        self.func = func
        self.retval = None


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
    BR_KILL = '9'
    BROAD_SIGNALS = [BR_KILL]

    TAG_AVAILABLE = 1
    TAG_JOB_TO_WORKER = 2
    TAG_JOB_TO_MASTER = 3

    ALL_TAG = [TAG_JOB_TO_WORKER, TAG_AVAILABLE, TAG_JOB_TO_MASTER]

    def __init__(self, n_proc=None):
        """
        :param n_proc: If None, probably an exec loop
                    (Could start synchrone job processing if > 1 will run asynchrone)

        """
        # self.job_queue = queue.Queue()
        self.job_queue = queue.Queue()
        self.nproc = n_proc
        self.worker_pool = queue.Queue()
        self.waiting_worker = queue.deque()
        self.running_worker = queue.deque()
        self.running_jobs = queue.deque()
        self.completed_jobs = queue.Queue()
        self.worker_monitor_thread = None
        self.job_monitor_thread = None
        self.icomm = None
        self._is_close = False
        self._all_thread = []
        self.all_done = False
        self.stop_threads = False
        self._seep_time = 1

    def close(self):
        self._is_close = True

    def join(self):
        """ Joins force closing the queue!

        :return:
        """
        self._is_close = True
        self.job_queue.join()

        logging.debug('waiting for end')
        self.job_queue.join()
        logging.debug('pooling shutdown')
        self.terminate()
        logging.info('job done')

    def map_async(self, func, iterable):
        for args in iterable:

            self.job_queue.put_nowait(Job(args=args, func=func))

        self.start()

        return Results(self.completed_jobs)

    def start(self):

        # dill.dump_session()

        self.spawn_worker(n_process=self.nproc)
        logging.debug('launch add worker')
        self.worker_monitor_thread = threading.Thread(
            target=self.__add_worker_to_pool, args=(self.worker_pool,))
        self.worker_monitor_thread.start()
        self._all_thread.append(self.worker_monitor_thread)

        logging.debug('launch receive results')
        self.completed_jobs_thread = threading.Thread(
            target=self.__receive_results,
            args=(self.completed_jobs,))
        self.completed_jobs_thread.start()
        self._all_thread.append(self.completed_jobs_thread)

        logging.debug('launch send job')
        self.job_monitor_thread = threading.Thread(
            target=self.__send_job,
            args=(self.job_queue, self.worker_pool))
        self.job_monitor_thread.start()
        self._all_thread.append(self.job_monitor_thread)

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

    def __receive_results(self, completed_jobs):
        """ This loop recive completed jobs from workers and put then 
        in the completed_jobs Queue.
        
        :param completed_jobs: 
        :return: 
        """
        logging.info('receive loop on')
        while True:
            # blocking call
            logging.debug('request')
            job_request = self.icomm.irecv(source=MPI.ANY_SOURCE, tag=self.TAG_JOB_TO_MASTER)
            while not job_request.Get_status() and not self.stop_threads:
                logging.debug('waiting for done job {}'.format(job_request.Get_status()))
                sleep(self._seep_time)
            if self.stop_threads:
                break
            done_job = job_request.wait()
            if done_job is not None:
                logging.debug('adding done job {}'.format((done_job.func, done_job.args)))
                completed_jobs.put_nowait(done_job)
                self.job_queue.task_done()

        logging.info('receive loop off')

    def __add_worker_to_pool(self, worker_pool):
        """ This loop add available worker to worker_pool

        :param icomm:
        :param worker_pool:
        :return:
        """
        logging.info('worker loop on')
        while True:
            logging.debug('request')
            new_rank_request = self.icomm.irecv(source=MPI.ANY_SOURCE, tag=self.TAG_AVAILABLE)
            while not new_rank_request.Get_status() and not self.stop_threads:
                logging.debug('waiting for new workers {}'.format(new_rank_request.Get_status()))
                sleep(self._seep_time)
            if self.stop_threads:
                break
            new_rank = new_rank_request.wait()
            if new_rank is not None:
                logging.debug('adding worker {}'.format(new_rank))
                worker_pool.put_nowait(new_rank)

        logging.info('worker loop off')

    def __send_job(self, job_queue, worker_pool):
        """ Send Jobs form job_queue to workers in worker_pool.
        
        :param job_queue: 
        :param worker_pool: 
        :return: 
        """
        logging.info('job loop on')
        a_worker = None
        a_job = None
        while True:
            # when job is None, nothing left tp do!
            logging.debug('waiting for job  {}'.format(job_queue.qsize()))

            logging.debug('waiting for worker')
            # Blocking loop
            while a_worker is None:
                try:
                    a_worker = worker_pool.get(timeout=1)
                except queue.Empty:
                    pass
                if self.stop_threads:
                    break
            if self.stop_threads:
                break

            logging.debug('got worker {}'.format(a_worker))

            # Blocking loop
            while a_job is None:
                try:
                    a_job = job_queue.get(timeout=1)
                except queue.Empty:
                    pass
                if self.stop_threads:
                    break
            if self.stop_threads:
                break

            logging.debug('{} {}'.format(a_worker, a_job))
            logging.debug('sending {} to {}'.format(a_job, a_worker))
            self.icomm.send(a_job, dest=a_worker, tag=self.TAG_JOB_TO_WORKER)
            a_worker = None
            a_job = None
            logging.debug('sent to worker {}'.format(a_worker))


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
        # TODO kill daemon thread if any
        self.all_done = True
        logging.info('broadcasting death')
        self.icomm.bcast(self.BR_KILL, root=MPI.ROOT)
        logging.debug('stoping thread')
        self.stop_threads = True
        # logging.debug('disconnect')
        # self.icomm.Disconnect()
        # logging.debug('sleep a bit')
        # sleep(3)

    def spawn_worker(self, n_process):
        """Sends jobs to slaves
        :return:
        """

        logging.info('Spawning worker form {}!'.format(socket.gethostname()))

        # TODO spawn many comm with one process each instead of many process with one comm

        self.icomm = MPI.COMM_WORLD.Spawn(
            sys.executable, args=[os.path.realpath(__file__)], maxprocs=n_process)

        logging.info('spawned {} mpi workers'.format(n_process))

    def start_thread(self, func, args=None, the_thread=None):
        """ Convenience method to start a thread with func

        :param the_thread: A handler on the thread
        :param func: the function to be run as a thread
        :return: the_thread
        """
        logging.info('adding thread {}'.format(func))
        if the_thread is None and not func.__name__.startswith('_'):
            m = ('the threadable function has to start with at least one "_" ')
            raise NameError()

        _the_thread = threading.Thread(target=func, args=args)
        _the_thread.start()
        self._all_thread.append(_the_thread)

        if the_thread is None:
            thread_name = '{}_thread'.format(func.__name__.lstrip('_'))
            logging.debug('adding thread {} to self.{}'.format(func, thread_name))
            setattr(self, thread_name, _the_thread)
        else:
            the_thread = _the_thread

        logging.debug('thread started')
        return _the_thread


class Results(object):
    def __init__(self, completed_queue):
        self._completed_queue = completed_queue
        self._completed = []

    @property
    def completed(self):
        ''' Make a complete list

        :return:
        '''
        while True:
            try:
                self._completed.append(self._completed_queue.get_nowait())
            except queue.Empty:
                break

        return self._completed

    @completed.setter
    def completed(self, value):
        self._completed.append(value)

    def get(self):

        return [j.retval for j in self.completed]


class MPIWorker(MpiPool):

    def __init__(self, *args, **kwargs):
        super(MPIWorker, self).__init__(*args, **kwargs)

        # dill.load_session()

        self.hostname = socket.gethostname()
        try:
            self.icomm = MPI.Comm.Get_parent()
            self.rank = self.icomm.Get_rank()
        except Exception:
            raise ValueError('Could not connect to parent - ')

        logging.debug('launch add worker')

        # self.broadcast_listener_thread = self.start_thread(self.__broadcast_listener)
        self.broadcast_listener_thread = threading.Thread(target=self.__broadcast_listener)
        self.broadcast_listener_thread.start()
        self._all_thread.append(self.broadcast_listener_thread)

    def __broadcast_listener(self):
        message = None
        logging.debug('listening')
        while True:
            # Blocking
            message = self.icomm.bcast(message, root=0)
            logging.info('received message {}'.format(message))
            if message not in self.BROAD_SIGNALS:
                logging.warning('Broadcast message {} unknown'.format(message))
            elif message is self.BR_KILL:
                self._auto_kill()
                break

    def exec_pool(self):
        """Execute jobs sent from master
        Will exit if none is received.
        """

        logging.info('worker {} running'.format(self.rank))

        # log = 'I AM {}, rank {}'.format(hostname, rank)
        # self.comm.gather(sendobj=log, root=0)
        i = 0
        while True:

            logging.info('worker {} available'.format(self.rank))
            self.icomm.send(self.rank, dest=0, tag=self.TAG_AVAILABLE)
            # wait for things to do

            logging.debug('worker {} waiting for new job'.format(self.rank))
            # blocking
            the_job = self.icomm.recv(source=0, tag=self.TAG_JOB_TO_WORKER)
            # self.icomm.send(33, dest=0, tag=33)

            logging.debug('worker {} got job{}'.format(self.rank, the_job))

            if the_job is None:
                # No more jobs to do
                break

            function = the_job.func
            args = the_job.args
            logging.debug(function)
            logging.debug(args)
            try:
                fct_ret_val = function(args)
                status = self.DONE
            except Exception as e:
                import traceback
                logging.error(e.message)
                logging.error(function)
                logging.error(args)

                fct_ret_val = None
                status = self.FAILED
            the_job.retval = fct_ret_val
            the_job.status = status

            logging.info('{} finish with status {}'.format(the_job.func, status))
            logging.debug('sending results back to master')
            self.icomm.send(the_job, dest=0, tag=self.TAG_JOB_TO_MASTER)

            i += 1
            logging.debug('rank = {} job = {} loop num = {}'.format(self.icomm.rank, (the_job.func, the_job.args), i))

        logging.info('worker {} on {} terminating'.format(self.rank, self.hostname))

    def com_cleaup(self):

        for comm_tag in self.ALL_TAG:
            self.icomm.send(None, dest=0, tag=comm_tag)

        self.icomm.Disconnect()

    def _auto_kill(self):
        """Kill current process and its children

        :return:
        """
        pid = os.getpid()
        me = psutil.Process(pid)
        logging.info('Killing children process')
        for proc in me.children(recursive=True):
            proc.kill()
        # auto destroy
        logging.info('shutting down rank {}'.format(self.rank))
        self.com_cleaup()
        os._exit(0)


def local_one(*args):
    hostname = socket.gethostname()
    log = 'I am on {},  and this is local_one processing these args {}'.format(hostname, args)
    logging.info(log)
    # sleep(.5)
    return '{}\n and these are your args\n {}'.format(log, args)


def test_mpi_pool(n_proc=1):

    pool = MpiPool(n_proc=n_proc)
    a = [[j for j in range(random.randint(1, 5))] for i in range(33)]
    args = [(1, 2, 3), ("nous", "allons", "aux", "bois"),]+a

    r = pool.map_async(local_one, args)

    # blocking statement
    # TODO put a blocking statement that can Show a progress status with the job and workers
    pool.join()
    logging.info('results are: {}'.format(r.get()))


def main(args=None):

    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(description='A Master Worker MPI setup')
    parser.add_argument("--mode", default='worker')
    parser.add_argument("-n", "--np", default=1, type=int)
    parsed = parser.parse_args(args)

    FORMAT = "%(levelname)7s --%(lineno)5s %(funcName)25s():  %(message)s"

    if parsed.mode == 'worker':
        logging.basicConfig(level=logging.INFO, format=FORMAT, filename='/tmp/worker.log')
        # Use a file handle when more than one worker !!!
        worker = MPIWorker()
        worker.exec_pool()
    else:
        logging.basicConfig(level=logging.DEBUG, format=FORMAT, stream=sys.stdout)
        test_mpi_pool(n_proc=parsed.np)


if __name__ == '__main__':
    main()

