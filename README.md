# poneyexpress
Batch processor for localhost or HPC system based on mpi

It offers an interface similar to the one of multiprocessing and multithreading to run python method in parallel. 


Usage:

import poneyexpress

def a_func(some_input):
    do_stuff()

pool = poneyexpress.MpiPool(n_proc=2)

async_results = pool.map_mpi(a_func, [input_1, input_2])

pool.join()
