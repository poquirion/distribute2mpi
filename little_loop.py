#!/usr/bin/env python
from mpi4py import MPI
from random import random
from time import sleep
import sys
import numpy
test_dir = "/home/poq/mpi4py_test"
comm = MPI.COMM_WORLD
mode = MPI.MODE_WRONLY|MPI.MODE_CREATE#|MPI.MODE_APPEND
# fh = MPI.File.Open(comm, "{0}/logfile.log".format(test_dir), mode)
# fh.Set_atomicity(True)

# for i in range(33):
#     t = random() + 0.001
#     msg = "{0} iter {1} - begin\n".format(comm.rank, i)
#     fh.Write_shared(msg)
#     sleep(t)
#     msg = "{0} iter {1} - end\n".format(comm.rank, i)
#     fh.Write_shared(msg)
#
# fh.Sync()
# fh.Close()

size = comm.Get_size()

def master_loop(job_list):
    '''
    Sends jobs to slaves
    :return:
    '''
    Done = False
    rec_val = None
    status = MPI.Status()
    avail_proc = [i for i in range(1, size)]
    for i, t in enumerate(job_list):

        comm.send((i,t), avail_proc.pop())

        if avail_proc == []:
            comm.recv(status=status)
            a = status.Get_source()
            avail_proc.append(int(a))

    for i in range(1, size):
        comm.send(None, i)

def exec_pool():
    """
    Execute jobs sent from master
    """

    while True:
        val = comm.recv(source=0)
        if val is None:
            break
        sleep(val[1])
        # init_cast = None
        # init_cast = comm.scatter(init_cast, root=0)
        comm.send(True, 0)
        print('rank = {} loop_num = {} value = {}'.format(comm.rank, val[0], val[1]))

if comm.rank == 0:

    master_loop(numpy.random.randint(2, size=33)
)
else:
    exec_pool()

