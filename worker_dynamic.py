#!/usr/bin/env python

import mpi4py.MPI
intra = mpi4py.MPI.COMM_WORLD
port = mpi4py.MPI.Lookup_name('test')
print ('connecting')
inter = intra.Connect(port)
print ('connected')

rank = inter.Get_rank()

print(rank)

inter.send(rank, dest=0)


print(inter.recv(source=0))
