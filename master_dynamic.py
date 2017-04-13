#!/usr/bin/env python

import mpi4py.MPI
intra = mpi4py.MPI.COMM_WORLD
port = mpi4py.MPI.Open_port()
port_name = 'test'
mpi4py.MPI.Publish_name(port_name, port)
print('waiting')
inter = intra.Accept(port)
print('connected')

worker_rank = inter.recv(source=mpi4py.MPI.ANY_SOURCE)

print (worker_rank)

inter.isend('prout', worker_rank)

print('BYE')